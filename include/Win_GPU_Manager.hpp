/* ***************************************************************************
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License version 3 as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 *  License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 ****************************************************************************
 *
 * Athor: Tiziano De Matteis <dematteis at di.unipi.it>
 */


/**
  This file contains the definition of the Manager, which is in charge
  of finding the right operating configuration
*/




#ifndef WIN_GPU_MANAGER_HPP
#define WIN_GPU_MANAGER_HPP
#include <thread>
#include <vector>
#include <map>
#include <set>
#include <gsl/gsl_qrng.h>
#include <ff/buffer.hpp>

//for interpolating
#include "../delaunay_linterp/src/delaunay_2_interp.h"
#include "Win_GPU_Config.h"
/**
  PLEASE PAY ATTENTION: compiling this using the delaunay_2_interp and nvcc -x cu will cause errors
  In particular undefined reference to `Eigen::MatrixBase<Eigen::Matrix<double, -1, -1, 0, -1, -1> >::partialPivLu() const
  To solve this:
	-	refactor the code moving the cuda kernel into separete files
	-	or (solution actually used) modify the Eigen/src/LU/PartialPivLU.h by removing the #ifnded _CUDACC_
			directives. It seems that the preprocess remove the coded even if the _CUDACC is defined
			For Cuda 9, comment in Eigen/src/Core/util/Macros.h all the _CUDACC_VER (otherwise compilation error)
*/

// Programming style: since  here the vast majority is private I've avoided to add an underscore the various variable names



/**
 * @brief The reconfiguration_message_t struct is the message sent from the manager to the emitter
* containing the information about the new configuration that must be adopted
*/
struct reconfiguration_message_t{

	int new_par_degree;
	int new_batch_size;


	reconfiguration_message_t(int pd, int bs)
	{
		new_par_degree=pd;
		new_batch_size=bs;
	}
}__attribute__((__aligned__(64)));

/**
* @brief The emitter_message_t struct is the message sent from the emitter to the manager
*
*/
struct emitter_message_t{
	bool eos;
	emitter_message_t(){}
	emitter_message_t(bool e)
		:eos(e){}
}__attribute__((__aligned__(64)));


/**
* @brief The worker_message_t struct contains the monitoring data sent by the workers to the manager
* NOTE: NOT USED
*/
struct worker_message_t{
	double rq;

	worker_message_t(double r)
	{
		rq=r;
	}

}__attribute__((__aligned__(64)));

struct collector_message_t{
	double thr;
	collector_message_t(){};
	collector_message_t(double t)
	{
		thr=t;
	}

}__attribute__((__aligned__(64)));




/**
 * @brief The Win_GPU_Manager class implements the Manager thread.
 *
 * NOTE: for the moment being it accepts as construction parameter also the input rate that must be sustained
 * TODO: avoid to have the rate as parameter
 */
class Win_GPU_Manager
{

public:

	Win_GPU_Manager(ff::SWSR_Ptr_Buffer *from_em,ff::SWSR_Ptr_Buffer *to_em,std::vector<ff::SWSR_Ptr_Buffer *> &from_w,
					ff::SWSR_Ptr_Buffer * from_coll,int starting_pd, int starting_bs, int rate)
		:from_emitter(from_em),to_emitter(to_em),from_workers(from_w),from_collector(from_coll),starting_par_degree(starting_pd),
		  starting_bs(starting_bs), rate(rate)
	{
		//print the legend

		printf("[Manager] colored prints legend: " CYAN " results (throughput) from a configuration\n" RESET);
		printf(CYAN_BOLD "                                  testing a random configuration\n" RESET);
		printf(GREEN_BOLD "                                  testing a possible optimal configuration\n" RESET);
		printf(RED_BOLD "                                  results from an optimal configuration are not good (go on with other configurations or backtracking)\n "RESET);

		//fill the batches possibilities
		for(int i=MAX_CUDA_THREADS_PER_BLOCK; i<=MAX_BATCH_SIZE;i+=MAX_CUDA_THREADS_PER_BLOCK)
			batches.push_back(i);

		//fill the worker possibilities
		for(int i=1; i<=MAX_NUM_WORKERS;i++)
			n_workers.push_back(i);

		//TODO: cleanup
//		for(auto i:batches)
//			printf("%d\n",i);
//		for(auto i:n_workers)
//			printf("%d\n",i);

	}

	~Win_GPU_Manager()
	{
		//join the thread
		thread.join();
	}

	/**
	 * @brief start: starts the Manager thread
	 */
	void start()
	{
		thread=std::thread(&Win_GPU_Manager::WF_Manager,this,starting_par_degree, starting_bs);

		//it works better without affinity (no affinity in VLDB tests)
		/*cpu_set_t cpuset;
		CPU_ZERO(&cpuset);
		CPU_SET(affinity, &cpuset);
		int rc = pthread_setaffinity_np(thread.native_handle(),sizeof(cpu_set_t), &cpuset);
		if (rc != 0) {
			std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
		}*/
	}



	/*
	* The Manager loop: for the moment being it receives the incoming rate (in terms of triggerable computations per second)
	*	and takes decisions
	*/
	void WF_Manager(int starting_pd, int starting_bs)
	{


		bool eos=false;
		emitter_message_t *emitter_msg=nullptr;
		collector_message_t *collector_msg=nullptr;
		int par_degree=starting_pd;
		int batch_s=starting_bs;
		int tested_configurations=0;
		//needed for the interpolation
		Delaunay_incremental_interp_2 adaptive_triang;
		std::array<double, 2> args;
		std::vector<double> stabilization_times{1000};				//keep track on the time that passes between the triggering of a reconfiguration to when we receive its stable througput
		configuration_t target_conf;								//variable used to look at a possible optimal configuration

		bool test_target_configuration=false;						//indicates whether or not we are testing a possible target configuration
		int next_interesting_step;									//the next interesting step (e.g. the one in which we have to find an optimal configuration)


		printf(RED_UNDERLINE "[Manager] expected throughput:%d \n" RESET,rate);


		//generate all the needed configurations
		generate_configurations();


		//for testing the reconfiguration mechanims, we will generate only diffent configurations with the same batch
		//generate_configurations_for_testing(par_degree,batch_s);

		long start_t=ff::getusec();
		long manager_start_time=start_t;

		//start the cycle: please note that the program starting configuration is the one with the
		//lowest number of workers and smallest benchmark (the first configuration in the list)

		std::map<configuration_t,throughput_configuration_t,classcomp>::iterator it =configurations.begin();

#if defined (LOG_MANAGER)
		FILE *flog=fopen("manager.log","w");
		fprintf(flog, "Logging format: [time in sec][phase] action\n");
		fprintf(flog, "Rate to sustain: %d\n",rate);

#endif

#if defined (BRUTE_FORCE)
		next_interesting_step=-1;		//in this case we will simply test all the possible configurations
#else
		next_interesting_step=INITIAL_CONFIG;	//the id of the next interisting configuration
#endif
		while(!eos && it!=configurations.end())
		{
			//receive from the collector or eos from emitter
			if(!receive_from_collector_or_emitter(from_emitter,&emitter_msg,from_collector,&collector_msg))
				eos=emitter_msg->eos;

			if(eos) break;

#if defined (BRUTE_FORCE)
			args[0]=it->first.nw;
			args[1]=it->first.batch;
			//in this case we do not perform interpolation
			stabilization_times.push_back((ff::getusec()-start_t)/1000.0);
			printf(RED "[Manager] Configuration (%d, %d) is stable. Measured throughput %.3f, Stabilization time: %.1f\n" RESET, (int)args[0],(int)args[1], collector_msg->thr,stabilization_times.back());
			it->second.measured_thr=MIN(collector_msg->thr,rate);
			tested_configurations++;
#else

			if(!test_target_configuration)
			{
				//EXPLORATION PHASE

				//Save the time
				stabilization_times.push_back((ff::getusec()-start_t)/1000.0);

				//the measured throughput could be higher than the real one maybe becase some tuples
				//were previously enqueued and now we are processing them. Take the minimum between measured and expected rate
				it->second.measured_thr=MIN(rate,collector_msg->thr);
				//update the interpolation
				update_interpolation(adaptive_triang,it->first.nw, it->first.batch,MIN(rate,collector_msg->thr));
				tested_configurations++;

				printf(CYAN "[Manager] Step: %d Configuration (%d, %d) is stable. Measured throughput %.3f Stabilization time: %.1f\n" RESET, tested_configurations, it->first.nw, it->first.batch,it->second.measured_thr,stabilization_times.back());

#if defined (LOG_MANAGER)
				fprintf(flog,"[%.3f][Exploration] Step: %d Configuration (%d, %d) is stable. Measured throughput %.3f Stabilization time(msec): %.1f\n", (ff::getusec()-manager_start_time)/1000000.0, tested_configurations, it->first.nw, it->first.batch,it->second.measured_thr,stabilization_times.back());
#endif

				//Used for testing the interpolation: at fixed interval it prints all the expected rates using the interpolation accumulated so far
				//if(tested_configurations%5==0)
				//	dump_interpolation_to_file(adaptive_triang,tested_configurations,rate,(ff::getusec()-manager_start_time)/1000000.0);
			}
			else
			{
				//TARGETING PHASE
				//we were testing a possible optimal configuration. Let's see what happened
				tested_configurations++;

				//update the tested configuration: search it
				auto this_conf=search_configuration(target_conf.nw, target_conf.batch);
				this_conf->second.measured_thr=MIN(rate,collector_msg->thr);

				//update the interpolation
				update_interpolation(adaptive_triang,target_conf.nw, target_conf.batch,MIN(collector_msg->thr,rate));

				//check that it satisfies our requirement
				if(abs(this_conf->second.measured_thr-rate)/rate<=TOLERANCE_CHECK)
				{
					printf(CYAN "[Manager] Step: %d Target Configuration (%d, %d) is stable. Measured throughput %.3f. Stabilization time: %.1f\n" RESET,tested_configurations, target_conf.nw, target_conf.batch, this_conf->second.measured_thr,stabilization_times.back());
#if defined(LOG_MANAGER)
					fprintf(flog,"[%.3f][Targeting-1] Step: %d Target Configuration (%d, %d) is stable. Measured throughput %.3f. Stabilization time(msec): %.1f\n", (ff::getusec()-manager_start_time)/1000000.0,tested_configurations, target_conf.nw, target_conf.batch, this_conf->second.measured_thr,stabilization_times.back());
#endif

					//If it satisfies the rate rquirement, search for another target configuration. If it is different and better than this one apply it
					//TODO: per ora better si intende con <= worker e <=batch
					//ATTENZIONE: potrebbe essere sbagliata e ritornare in exploring phase
					find_target_throughput_configuration(adaptive_triang,target_conf,rate,TOLERANCE_SEARCH);
					if(target_conf.nw!=0 && target_conf.nw!=this_conf->first.nw || target_conf.batch!=this_conf->first.batch)
					{

						if(target_conf.nw<=this_conf->first.nw && target_conf.batch<=this_conf->first.batch)
						{
							test_target_configuration=true;
							while(!to_emitter->push((void *)new reconfiguration_message_t(target_conf.nw,target_conf.batch)));
							printf(GREEN_BOLD  "[Manager] Step: %d. Refining: Set operating point to possible target (this will be the %d-th configuration): (%d,%d)\n" RESET,tested_configurations,tested_configurations+1,target_conf.nw,target_conf.batch);
#if defined(LOG_MANAGER)
							fprintf(flog,"[%.3f][Targeting-1]Step: %d. Refining: Set operating point to possible target (this will be the %d-th configuration): (%d,%d)\n" , (ff::getusec()-manager_start_time)/1000000.0,tested_configurations,tested_configurations+1,target_conf.nw,target_conf.batch);
#endif
						}
						else
							break;
					}
					else
						break;

				}
				else
				{
					//the tested configuration does not sustain the rate. Returns to Exploring Phase
					printf(RED_BOLD "[Manager] Step %d. Target Configuration (%d, %d) is NOT SUFFICIENT Go on with other random configurations!!!. Measured throughput %.3f, Stabilization time: %.1f\n" RESET, tested_configurations, target_conf.nw, target_conf.batch,this_conf->second.measured_thr,stabilization_times.back());
#if defined(LOG_MANAGER)
					fprintf(flog,"[%.3f][Targeting-1] Step %d. Target Configuration (%d, %d) is NOT SUFFICIENT Go on with other random configurations!!!. Measured throughput %.3f, Stabilization time: %.1f\n" , (ff::getusec()-manager_start_time)/1000000.0, tested_configurations, target_conf.nw, target_conf.batch,this_conf->second.measured_thr,stabilization_times.back());
#endif
					test_target_configuration=false;
					next_interesting_step=tested_configurations+ADDITIONAL_CONFIG;
				}
			}

#endif

			if(tested_configurations==next_interesting_step)
			{
				//we tested a certain amount of configuration: we can try to find an optimal configuration
				find_target_throughput_configuration(adaptive_triang,target_conf,rate,TOLERANCE_SEARCH);

				//if we found it
				if(target_conf.nw!=0)
				{
					test_target_configuration=true;	//switch to Targeting Phase
					while(!to_emitter->push((void *)new reconfiguration_message_t(target_conf.nw,target_conf.batch)));
					printf(GREEN_BOLD  "[Manager] Step %d. set operating point to possible target (this will be the %d-th configuration): (%d,%d)\n" RESET,tested_configurations, tested_configurations+1,target_conf.nw,target_conf.batch);
#if defined(LOG_MANAGER)
					fprintf(flog,"[%.3f][Exploring] Switching to Targeting-1:  Step %d. set operating point to possible target (this will be the %d-th configuration): (%d,%d)\n",(ff::getusec()-manager_start_time)/1000000.0,tested_configurations, tested_configurations+1,target_conf.nw,target_conf.batch);
#endif
				}
				else
				{
					//otherwise, come back to the random ones
					printf(RED_BOLD "[Manager] Step: %d. Failed to find optimal configuration\n" RESET,tested_configurations);
#if defined(LOG_MANAGER)
					fprintf(flog,"[%.3f][Exploring] Step: %d. Failed to find optimal configuration\n",(ff::getusec()-manager_start_time)/1000000.0, tested_configurations);
#endif
					next_interesting_step+=ADDITIONAL_CONFIG;
				}
			}
			if(!test_target_configuration){

				it++;
				//get the next valid configuration (i.e. not already tested)
				while(it!=configurations.end() && it->second.measured_thr!=0)
					it++;

				if(it!=configurations.end())
				{
					//send it to the emitter
					par_degree=it->first.nw;
					batch_s=it->first.batch;
					while(!to_emitter->push((void *)new reconfiguration_message_t(par_degree,batch_s)));
					printf(CYAN_BOLD  "[Manager] Step: %d. Set operating point to: (%d,%d)\n" RESET,tested_configurations, par_degree,batch_s);
#if defined(LOG_MANAGER)
					fprintf(flog,"[%.3f][Exploring] Step: %d. Set operating point to: (%d,%d)\n",(ff::getusec()-manager_start_time)/1000000.0,tested_configurations, par_degree,batch_s);
#endif

				}
			}

			//cleanup
			if(!eos)
				delete collector_msg;
			start_t=ff::getusec();

		}

		printf(CYAN "[Manager] finished. Tested %d configurations\n",tested_configurations);
		printf(CYAN_BOLD"------------------------------------------------------------\n"RESET);
		printf(CYAN "[Manager] Try second euristic: current configuration (%d,%d)\n" RESET,target_conf.nw, target_conf.batch);

#if defined(LOG_MANAGER)
		fprintf(flog,"[%.3f][Exploring] finished. Tested %d configurations\n", (ff::getusec()-manager_start_time)/1000000.0,tested_configurations);
		fprintf(flog,"------------------------------------------------------------\n");
		fprintf(flog,"[%.3f][Targeting-2] Try second euristic: current configuration (%d,%d)\n",(ff::getusec()-manager_start_time)/1000000.0,target_conf.nw, target_conf.batch);

#endif

		/**
			Second euristic: improve the actual configuration by using raindrop
		*/


		configuration_t current_conf=target_conf;
		configuration_t prev_conf;

		while(!eos && it!=configurations.end())
		{
			stabilization_times.push_back((ff::getusec()-start_t)/1000.0);

			//the idea is to look at the neighbours of the current configuration (only the ones with less nw and batch)
			//and pick up the best one according to the current interpolation. We will apply that configuration: if it is ok
			//we re-iterate otherwise we do back-track to the previous configuration


			//find the best neighbor
			configuration_t next_conf=current_conf;

			find_best_neighbor(adaptive_triang,current_conf,next_conf);

			if(next_conf.nw==current_conf.nw && next_conf.batch==current_conf.batch)
			{
				printf(CYAN "[Manager] Euristic2: No best configurations found\n"RESET);
#if defined(LOG_MANAGER)
				fprintf(flog,"[%.3f][Targeting-2] Euristic2: No best configurations found\n", (ff::getusec()-manager_start_time)/1000000.0);
#endif
				break;
			}

			//TODO handle the case in which we already tested that configuration

			//save the previous conf for backtracking
			prev_conf=current_conf;
			current_conf=next_conf;

			while(!to_emitter->push((void *)new reconfiguration_message_t(current_conf.nw,current_conf.batch)));
			printf(GREEN_BOLD  "[Manager] Step: %d. Set operating point to possible target (this will be the %d-th configuration): (%d,%d)\n" RESET,tested_configurations, tested_configurations+1,current_conf.nw,current_conf.batch);
#if defined(LOG_MANAGER)
			fprintf(flog,"[%.3f][Targeting-2] Step: %d. Set operating point to possible target (this will be the %d-th configuration): (%d,%d)\n",(ff::getusec()-manager_start_time)/1000000.0,tested_configurations, tested_configurations+1,current_conf.nw,current_conf.batch);
#endif

			//receive from the collector or eos from emitter
			if(!receive_from_collector_or_emitter(from_emitter,&emitter_msg,from_collector,&collector_msg))
				eos=emitter_msg->eos;

			if(!eos)
			{

				tested_configurations++;
				stabilization_times.push_back((ff::getusec()-start_t)/1000.0);

				//update the tested configuration: search it
				auto this_conf=search_configuration(current_conf.nw, current_conf.batch);

				this_conf->second.measured_thr=MIN(rate,collector_msg->thr);

				//update the interpolation
				update_interpolation(adaptive_triang,current_conf.nw, current_conf.batch,MIN(collector_msg->thr,rate) );

				//check that it satisfies our requirement
				if(abs(this_conf->second.measured_thr-rate)/rate<=TOLERANCE_EURISTIC2)
				{
					printf(CYAN "[Manager] Step: %d. This Configuration (%d, %d) is stable. Measured throughput %.3f.  Stabilization time: %.1f\n" RESET, tested_configurations, current_conf.nw, current_conf.batch, this_conf->second.measured_thr,stabilization_times.back());
#if defined(LOG_MANAGER)
					fprintf(flog,"[%.3f][Targeting-2]  Step: %d. This Configuration (%d, %d) is stable. Measured throughput %.3f.  Stabilization time: %.1f\n",(ff::getusec()-manager_start_time)/1000000.0, tested_configurations, current_conf.nw, current_conf.batch, this_conf->second.measured_thr,stabilization_times.back());
#endif
					//this is ok we can re-iterate this thing
				}
				else
				{
					//backtrack
					printf(RED_BOLD "[Manager] Step %d. Target Configuration (%d, %d) is NOT SUFFICIENT!!!. Mesured Throughput: %.3f. Back track to (%d, %d)\n" RESET, tested_configurations, current_conf.nw, current_conf.batch, this_conf->second.measured_thr,prev_conf.nw, prev_conf.batch);
#if defined(LOG_MANAGER)
					fprintf(flog,"[%.3f][Targeting-2]  Step %d. Target Configuration (%d, %d) is NOT SUFFICIENT!!!. Mesured Throughput: %.3f. Back track to (%d, %d)\n", (ff::getusec()-manager_start_time)/1000000.0, tested_configurations, current_conf.nw, current_conf.batch, this_conf->second.measured_thr,prev_conf.nw, prev_conf.batch);
#endif
					while(!to_emitter->push((void *)new reconfiguration_message_t(prev_conf.nw,prev_conf.batch)));
					break;

				}
			}
		}

		printf(CYAN_BOLD "[Manager] finished second conf. Tested %d configurations\n" RESET,tested_configurations);

#if defined(LOG_MANAGER)
		fprintf(flog,"[%.3f][Targeting-2] finished second conf. Tested %d configurations\n",(ff::getusec()-manager_start_time)/1000000.0,tested_configurations);
		fclose(flog);
#endif
		//print all the measured throughput so far
		//	printf("----\n");

		FILE *fout=fopen("throughput.dat","w");
#if defined (BRUTE_FORCE)
		fprintf(fout,"#Brute force manager: all configurations have been tested\n");
		fprintf(fout,"#NW\tBATCH\tThr\tStabTime(ms)\n");
		int i=0;
		for(std::pair<configuration_t,throughput_configuration_t> a:configurations)
		{
			fprintf(fout,"%d\t%d\t%.1f\t%.1f\n",a.first.nw, a.first.batch,a.second.measured_thr, stabilization_times[i++]);
		}
#else
		fprintf(fout,"#NW\tBATCH\tThr\tExpTh\tError\tStabTime(ms)\n");
		int i=0;
		for(std::pair<configuration_t,throughput_configuration_t> a:configurations)
		{
			fprintf(fout,"%d\t%d\t%.1f\t%.1f\t%.3f\t%.1f\n",a.first.nw, a.first.batch,a.second.measured_thr,a.second.expected_thr, std::abs(MIN(a.second.expected_thr,rate)-a.second.measured_thr)/a.second.measured_thr,stabilization_times[i++]);
		}
#endif
		fclose(fout);
	}

private:

	/*----------------------------------------------------------------------------------------------------

		Constants and type definitions

	-------------------------------------------------------------------------------------------------------*/


	//we used struct just to help code reading (by looking at members name)
	struct configuration_t{
		int id;			//the id represent the number of the configuration (used for ordering)
		int nw;			//the number of workers
		int batch;		//the batch size
	};

	struct throughput_configuration_t{
		double expected_thr;	//the throughput estimated with the interpolation
		double measured_thr;	//the real measured throughput (if different than zero the configuration has been already evaluated)
	};

	//Comparator used for map ordering
	struct classcomp {
		bool operator() (const configuration_t& a, const configuration_t& b) const
		{return a.id<b.id;}
	};

	std::vector<int> batches;	//contains the possible batch configurations
	std::vector<int> n_workers; //contains the possible workers configurations
	std::map<configuration_t,throughput_configuration_t,classcomp> configurations;

	/*----------------------------------------------------------------------------------------------------

		Methods definitions

	-------------------------------------------------------------------------------------------------------*/

	/**
	 * @brief generate_configurations generates all the possible configurations that have to be tested
	 * In case of a Brute Force approach they are generated simply in order
	 * In case of a Managers that tries to guess the optimal configuration they are generated using a
	 * small discrepancy random generator
	 */
	void generate_configurations()
	{
		std::set<std::pair<int,int>> generated_configuration; //used to skip duplicates
		int num_conf=0;


#if defined(BRUTE_FORCE)
		//used for testing: generate all the possible configurations
		//it will be used to see the actual behaviour of the program and
		//take the real optimum
		//NOTE: no interpolation will be used in this case

		configuration_t  conf;

#if defined(PARTIAL)
		//we will test only part of the configuration
		const int perc_keep=20; //keep the 20% (more or less)
		srand(0);
#endif
		for(int nw:n_workers)
		{
			for(int b:batches)
			{
				conf.id=num_conf++;
				conf.nw=nw;
				conf.batch=b;
#if defined(PARTIAL)
				int n=rand()%100;
				if(n>=perc_keep)
					continue;	//skip this
#endif
				configurations[conf]={0.0,0.0};
				generated_configuration.insert({conf.nw,conf.batch});
			}
		}
#if defined(PARTIAL)
		printf("[Manager] Only %d out of %d configurations will be evaluated\n",generated_configuration.size(),n_workers.size()*batches.size());
#endif
#else
		gsl_qrng * q = gsl_qrng_alloc (gsl_qrng_sobol, 2);
		int num_conf_to_generate=n_workers.size()*batches.size();
		//first of all insert the boundaries
		configuration_t  conf={num_conf++,n_workers.front(),batches.front()};
		configurations[conf]={0.0,0.0};
		generated_configuration.insert({conf.nw,conf.batch});
		conf={num_conf++,n_workers.front(),batches.back()};
		configurations[conf]={0.0,0.0};
		generated_configuration.insert({conf.nw,conf.batch});
		conf={num_conf++,n_workers.back(),batches.back()};
		configurations[conf]={0.0,0.0};
		generated_configuration.insert({conf.nw,conf.batch});
		conf={num_conf++,n_workers.back(),batches.front()};
		configurations[conf]={0.0,0.0};
		generated_configuration.insert({conf.nw,conf.batch});

#if defined(SOFT_RECONFIGURATIONS)
		//dunno why, but in the SOFT case we are not able to generate all the configurations using the random numb generator

		num_conf_to_generate-=10;
#endif

		while(configurations.size()<num_conf_to_generate)
		{
			double v[2];
			int batch, nw;
			gsl_qrng_get (q, v);
			nw=n_workers[(int)(v[0]*n_workers.size())];
			batch=batches[(int)(v[1]*batches.size())];
#if defined(SOFT_RECONFIGURATIONS)
			//For soft configurations we change one dimension at a time to avoid long waits
			if(nw!=conf.nw)
			{
				//check if it exists
				if(generated_configuration.find({nw,conf.batch})==generated_configuration.end()) //still not generated
				{
					conf.nw=nw;
					conf.id=num_conf++;
					configurations[conf]={0.0,0.0};
					generated_configuration.insert({conf.nw,conf.batch});
				}

			}
			if(batch!=conf.batch)
			{
				if(generated_configuration.find({conf.nw,batch})==generated_configuration.end()) //still not generated
				{
					conf.batch=batch;
					conf.id=num_conf++;
					configurations[conf]={0.0,0.0};
					generated_configuration.insert({conf.nw,conf.batch});
				}
			}
#else
			//for hard reconfigurations we can change both dimensions
			//check if it exists
			if(generated_configuration.find({nw,batch})==generated_configuration.end()) //still not generated
			{
				conf.batch=batch;
				conf.nw=nw;
				conf.id=num_conf++;
				configurations[conf]={0.0,0.0};
				generated_configuration.insert({conf.nw,conf.batch});
			}

#endif

		}
		printf("[Manager] generated %d configurations\n",num_conf_to_generate);
		gsl_qrng_free (q);


		//Print them
		//	std::map<configuration_t,throughput_configuration_t,classcomp>::iterator it =configurations.begin();
		//	while(it!=configurations.end())
		//	{
		//		printf("%d: %d, %d\n",it->first.id,it->first.nw,it->first.batch);
		//		it++;
		//	}
#endif
	}


	/**
	 * @brief generate_configurations_for_testing: generate all the possible workers configurations (the batch is fixed)
	 *			This is used for reconfiguration mechanisms testing
	 * @param start_pd
	 * @param start_bs
	 */
	void generate_configurations_for_testing(int start_pd, int start_bs)
	{
		std::set<std::pair<int,int>> generated_configuration; //used to skip duplicates
		int num_conf=0;

		configuration_t  conf={num_conf++,start_pd,start_bs};
		configurations[conf]={0.0,0.0};
		generated_configuration.insert({conf.nw,conf.batch});

		srand(time(NULL));
		//we have to generate all the configuration that can be obtained varying the number of workers
		while(configurations.size()<n_workers.size())
		{
			int next_w=rand()%n_workers.size()+1;
			if(generated_configuration.find({next_w,start_bs})==generated_configuration.end()) //still not generated
			{
				conf.batch=start_bs;
				conf.nw=next_w;
				conf.id=num_conf++;
				configurations[conf]={0.0,0.0};
				generated_configuration.insert({conf.nw,conf.batch});
			}
		}

		std::map<configuration_t,throughput_configuration_t,classcomp>::iterator it =configurations.begin();
		while(it!=configurations.end())
		{
			printf("%d: %d, %d\n",it->first.id,it->first.nw,it->first.batch);
			it++;
		}

	}

	/**
	 * @brief receive_from_collector_or_emitter blocking receive: it returns when a message from the Collector or the Emitter is  received
	 * @param from_emitter queue from the emitter
	 * @param emitter_msg
	 * @param from_collector
	 * @param collector_msg
	 * @return true when the message is received
	 */
	bool receive_from_collector_or_emitter(ff::SWSR_Ptr_Buffer* from_emitter, emitter_message_t **emitter_msg, ff::SWSR_Ptr_Buffer* from_collector, collector_message_t **collector_msg)
	{
		bool eos=false;
		bool received=false;
		while(!eos && !received)
		{
			if(from_collector->pop((void **)collector_msg))
				received=true;
			if(from_emitter->pop((void**)emitter_msg))
				eos=(*emitter_msg)->eos;
			else
				REPEAT_25( asm volatile("pause" ::: "memory");)
						//REPEAT_25( asm volatile("or 31,31,31   # very low priority");) //for IBM

		}

		return received;	//if we exit because of the eos this means that we have not received anything
	}



	/**
	 * @brief find_max_throughput_configuration return the best configuration (i.e. max throughput) considering the current interpolation
	 * @param interp
	 */
	void find_max_throughput_configuration(Delaunay_incremental_interp_2 &interp)
	{
		std::array<double, 2> args;
		long start_time=current_time_usecs();

		double max=0;
		configuration_t best_conf;
		for(int nw:n_workers)
		{
			for(int b:batches)
			{
				args[0]=nw;
				args[1]=b;

				double exp_thr=interp.interp(args.begin(), args.end());
				if(exp_thr>max)
				{
					best_conf.nw=nw;
					best_conf.batch=b;
					max=exp_thr;
				}
			}
		}
		printf("Best configuration: %d, %d. Expected thr: %.2f. Computation time: %ld\n",best_conf.nw, best_conf.batch, max, current_time_usecs()-start_time);
	}


	/**
	 * @brief find_target_throughput_configuration returns a configuration that approaches a given target (i.e. a given throughput) considering a certain
	 *	amount of tolerance.
	 *
	 *	Please note: for the moment being the first configuration (i.e. the one with the lowest number of workers) is returned
	 * @param interp
	 * @param target_conf
	 * @param target
	 * @param tolerance
	 */
	void find_target_throughput_configuration(Delaunay_incremental_interp_2 &interp, configuration_t &target_conf,double target, double tolerance)
	{
		std::array<double, 2> args;
		long start_time=current_time_usecs();
		target_conf.nw=0;
		target_conf.batch=0;


		//torniamo la prima configurazione che mi centra il target
		double exp_thr;
		std::map<configuration_t,throughput_configuration_t,classcomp>::iterator it;
		for(int nw:n_workers)
		{
			for(int b:batches)
			{

				//if the configuration has been already tested, take the measured throughput
				it=search_configuration(nw,b);
				if(it->second.measured_thr>0)
					exp_thr=MIN(it->second.measured_thr,target);
				else
				{
					args[0]=nw;
					args[1]=b;
					exp_thr=MIN(interp.interp(args.begin(), args.end()),target);
				}
				//if(abs(exp_thr-target)/target<=0.1)
				//printf("Configuration %d, %d: %.0f\n",nw,b,exp_thr);
				if(abs(exp_thr-target)/target<=tolerance)
				{
					target_conf.nw=nw;
					target_conf.batch=b;
					goto fine;
				}
			}
		}

fine:
		printf("**Target configuration: %d, %d. Expected thr: %.2f. Computation time: %ld\n",target_conf.nw, target_conf.batch, exp_thr, current_time_usecs()-start_time);
	}

	/**
	 * @brief dump_interpolation_to_file write the interpolation obtained for all
	* the different configuration so far. It is used for evaluating the error of the interpolation
	*
	*
	*/
	void dump_interpolation_to_file(Delaunay_incremental_interp_2 &interp,int num_points, int exp_rate, double time)
	{
		std::array<double, 2> args;
		char filename[100];
		sprintf(filename,"interpolation_%d",num_points);
		FILE *fout=fopen(filename,"w");


		//write all the configurations
		for(int nw:n_workers)
		{
			for(int b:batches)
			{
				args[0]=nw;
				args[1]=b;

				double exp_thr=interp.interp(args.begin(), args.end());
				//anche qui dovremmo limitare al rate in ingresso
				fprintf(fout,"%d\t%d\t%.3f\n",nw,b,MIN(exp_thr,exp_rate));
			}
		}
		fprintf(fout,"#Time from the start of the program: %.1f\n",time);
		fclose(fout);

	}


	/**
	 * @brief search_configuration serch for a given reconfiguration in the least of all generated configuration
	 * @return an iterator pointing to the configuration
	 */
	std::map<configuration_t,throughput_configuration_t,classcomp>::iterator search_configuration(int nw, int batch)
	{
		std::map<configuration_t,throughput_configuration_t,classcomp>::iterator it = configurations.begin();


		//for sure it exists
		while(it->first.nw!=nw || it->first.batch!=batch)
		{
			it++;
		}
		return it;

	}


	/**
	 * @brief update_interpolation update the interpolation using the information about the throughput of a given configuration
	 */
	void update_interpolation(Delaunay_incremental_interp_2 &interp, int nw, int batch, double thr)
	{
		std::array<double, 2> args;
		args[0]=nw;
		args[1]=batch;
		interp.insert(args.begin(),args.end(),thr);
	}

	/**
	 * @brief find_best_neighbor: used for the raindrop strategy
	 */
	void find_best_neighbor(Delaunay_incremental_interp_2 &adaptive_triang, configuration_t &current_conf, configuration_t &next_conf)
	{

		auto it_nw=std::find(n_workers.begin(),n_workers.end(),current_conf.nw);
		auto it_batch=std::find(batches.begin(), batches.end(),current_conf.batch);

		std::array<double, 2> args;
		double best_thr=0;
		double exp_thr;


		int length=1;	//how far search the next configuration (i.e. 1 step, 2 steps)
		int residual_nw=MIN(it_nw-n_workers.begin(),length); //understand how many possible configuration we have
		int residual_batch=MIN(it_batch-batches.begin(),length);
		for(int i=0;i<=residual_nw;i++)
		{
			for(int j=0;j<=residual_batch;j++)
			{
				if(i==0 && j==0) continue; //this is the current configuration

				args[0]=*(it_nw-i);
				args[1]=*(it_batch-j);
				//do not cut...trust the interpolation
				exp_thr=adaptive_triang.interp(args.begin(), args.end());
				//cut to rate
				//exp_thr=MIN(adaptive_triang.interp(args.begin(), args.end()),rate);
				//printf("Expected for %d %d: %.3f\n",args[0],args[1],exp_thr);
				if(exp_thr>best_thr)
				{
					best_thr=exp_thr;
					next_conf.nw=args[0];
					next_conf.batch=args[1];
				}

			}
		}

	}

	//variables

	//the thread
	std::thread thread;

	//argument passed
	int affinity;
	ff::SWSR_Ptr_Buffer *from_emitter;
	ff::SWSR_Ptr_Buffer *to_emitter;
	std::vector<ff::SWSR_Ptr_Buffer *> &from_workers;
	ff::SWSR_Ptr_Buffer * from_collector;
	int starting_par_degree;
	int starting_bs;
	int rate;
};
#endif // WIN_GPU_MANAGER_HPP
