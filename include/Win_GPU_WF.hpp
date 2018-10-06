
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
 ****************************************************************************
 *
 * Author: Tiziano De Matteis <dematteis at di unipi.it>
 *
 **/

#ifndef WIN_GPU_WF_HPP
#define WIN_GPU_WF_HPP

#include <tuple>
#include <vector>
#include <queue>
#include <time.h>
#include <math.h>
#include <iostream>
#include <functional>
#include <thread>
#include <ff/node.hpp>
#include <ff/farm.hpp>
#include <ff/pipeline.hpp>
#include "Win_GPU_Seq.hpp"
#include "Win_Allocators.hpp"
#include "Win_GPU_Manager.hpp"
#include "Win_GPU_Config.h"


#define MAX(a,b) (((a)>(b))?(a):(b))

//Defines for allocators
const size_t PER_WORKER_INPUT_QUEUE_LEN  = 262144;
const size_t PER_WORKER_OUTPUT_QUEUE_LEN =   6144;

using namespace ff;


int gcd(int a, int b)
{
	for (;;)
	{
		if (a == 0) return b;
		b %= a;
		if (b == 0) return a;
		a %= b;
	}
}

int lcm(int a, int b)
{
	int temp = gcd(a, b);
	return temp ? (a / temp * b) : 0;
}


/*! 
 * Win_GPU_WF implements Gaspow
 *
 */

template<typename IN_T, typename OUT_T, typename WIN_T,  typename CUDA_F_T, typename Ext_Alloc_IN_T=std::allocator<IN_T>, typename Ext_Alloc_OUT_T=std::allocator<OUT_T>>
class Win_GPU_WF: public ff_farm<> {
private:
	typedef IN_T in_tuple_t;
	typedef OUT_T out_tuple_t;
	typedef WIN_T win_type_t;
	typedef struct wrapper<in_tuple_t> tuple_wrapper_t;
	typedef struct wrapper<out_tuple_t> output_wrapper_t;
	typedef std::function<std::pair<unsigned int, unsigned long>(const in_tuple_t&)> FUNC_IN_T; // function to retrieve the key id and tuple id from an input tuple
	typedef std::function<std::pair<unsigned int, unsigned long>(const out_tuple_t&)> FUNC_OUT_T; // function to retrieve the key id and result if from an output result
	typedef typename win_type_t::win_container_t win_container_t;
	typedef std::function<void(out_tuple_t*, int , out_tuple_t* )> FUNC_H_T;	//For Pane version, this is the aggregation function that will be executed on the CPU (for the moment being)


	unsigned int _pardegree; // parallelism degree
	Ext_Alloc_IN_T* _ext_alloc_in; // pointer to an external allocator of input tuples
	Ext_Alloc_OUT_T* _ext_alloc_out; // pointer to an external allocator of output results
	bool freeAlloc=false; // true if the external allocators have been created into the pattern, false otherwise

	std::vector<cudaStream_t> _cuda_streams; //cuda streams, one for each working thread //TODO: possono essere quanti vogliamo, nel caso si possono limitare i thread?

	//variables required for the manager and the comunication entities/manager
	Win_GPU_Manager *manager;
	SWSR_Ptr_Buffer *_from_emitter_to_manager;
	SWSR_Ptr_Buffer *_from_manager_to_emitter;
	SWSR_Ptr_Buffer *_from_collector_to_manager;
	std::vector<SWSR_Ptr_Buffer *> _from_workers_to_manager;
	int _rate;			//for the moment being, the WF knows the incoming rate (expressed as the possible number of results computable per second, that is number of tuples per second / window slide)


public:

	/*
	*  \internal WF_Emitter
	*
	*  \brief The Emitter of the Window Farming Pattern
	*
	*  This is the Emitter of the Window Farming pattern that distrubutes
	*  each input tuple to one or more workers in such a way that the windows are distributed
	*  to the workers according to batch size B. B consecutive windows must be allocated to the
	*  same worker. The same tuple can be transmitted to multiple workers. This can be performed by copying it or sharing the tuple
	*  depending on the flag passed to the constructor.
	*
	*  This inner class is defined in \ref HLPP_patterns/include/Win_WF.hpp
	*/
	class WF_Emitter: public ff_node_t<in_tuple_t, tuple_wrapper_t> {
	private:
		FUNC_IN_T _M_IN; // function to retrieve the key id and tuple id from an input tuple
		unsigned long _win_length;
		unsigned long _win_slide;
		unsigned long _batch_size;
		unsigned int _pardegree; // number of workers
		ff_loadbalancer* const _lb;
		Ext_Alloc_IN_T* _ext_alloc_in; // pointer to the external allocator of input tuples
		unsigned long _totalReceived=0; // total number of tuples received
		unsigned long _totalCopies=0; // total number of copies for all the tuples received
		unsigned long _wb; // window length considering the batch (i.e. number of tuples that each worker must receive to trigger the computation)
		unsigned long _sb; // window slide considering batches
		unsigned int* _to_workers; // identifiers of the workers that will receive the input tuple (wrappered)
		Win_Allocator<in_tuple_t> *_int_alloc_in; // internal allocator of input tuples (one for all the workers)
		int _maximum_pardegree;

		//used for debugging and testing
		unsigned long start_time;
		//used for reconfiguration
		long int reconfiguration_tuple_id=-1;			//The tuple id from which we can change scheduling (according to the required reconfiguration)
		long int reconfiguration_last_id=-1;			//the last id of the tuple from which change the degree of parallelism
		long int reconfiguration_switch_tuple_id=-1;	//The tuple id from which we can change slide in older/remaining workers
		int new_num_worker;								//new number of workers (greater or less than the previous one)
		bool reconfiguring_pd=false;					//indicates whether or not we are in a reconfiguration phase for chaning the degree of parallelism
		bool reconfiguring_bs=false;					//indicates whether or not we are in a reconf. phase (change in the batch size (SOFT_RECONFIGURATIONS) or a general change (HARD_RECONFIGURATIONS))
		int new_batch_size;								//new batch size
		std::set<int> to_workers_set;					//in a reconf phase due to change of pardegree it is used to maintain the workers to which send a  given tuple
		std::vector<in_tuple_t> tuples_bs;				//tuples to be replied during the reconfiguration for batch size
		unsigned long starting_id=0;					//the first id of the received tuples. We have to keep track of it for reconfiguring the batch size (NOTE: must be one per key if we want to extend the support to multiple keys)

		//queues to/from manager
		SWSR_Ptr_Buffer *_to_manager;
		SWSR_Ptr_Buffer *_from_manager;
		reconfiguration_message_t *_reconf_msg;

		//monitoring
		std::vector<int> _reconf_tuples;		//number of tuples that elapses between the arrial of a reconfiguration request to its actually starting

		long _start_rec_t;

	public:
		// constructor
		WF_Emitter(FUNC_IN_T M_IN, long wlen, long wslide, long batch_size, unsigned int nw, ff_loadbalancer* const lb, Ext_Alloc_IN_T* ext_alloc_in,
				   int allocator_kind, SWSR_Ptr_Buffer *to_manager=nullptr, SWSR_Ptr_Buffer *from_manager=nullptr)
			:_M_IN(M_IN), _win_length(wlen), _win_slide(wslide), _batch_size(batch_size), _pardegree(nw), _maximum_pardegree(MAX_NUM_WORKERS),
			  _lb(lb), _ext_alloc_in(ext_alloc_in), _int_alloc_in(nullptr), _from_manager(from_manager),_to_manager(to_manager){
			_to_workers = new unsigned int[_maximum_pardegree];
			_wb = _win_length + (batch_size - 1) * _win_slide;
			_sb = _win_slide * batch_size;
			if (allocator_kind == 0) {// standard
				printf("EMITTER Using the standard allocator\n");
				_int_alloc_in = new Win_Allocator<in_tuple_t>();
			} else if (allocator_kind == 1) { // TBB
				//printf("EMITTER Using the TBB allocator\n");
				//_int_alloc_in = new Win_AllocatorTBB<in_tuple_t>();
				abort();
			} else if (allocator_kind == 2) { // FF
				printf("EMITTER Using the FF allocator (magic=%ld)\n", (PER_WORKER_INPUT_QUEUE_LEN/2)*_pardegree);
				_int_alloc_in = new Win_AllocatorFF<in_tuple_t>((PER_WORKER_INPUT_QUEUE_LEN/2)*_pardegree);
			} else {
				printf("EMITTER Using the STATIC allocator (magic=%ld)\n", (PER_WORKER_INPUT_QUEUE_LEN) * _pardegree);
				_int_alloc_in = new Win_StaticAllocator<in_tuple_t>((PER_WORKER_INPUT_QUEUE_LEN) * _pardegree);
			}

			start_time=ff::getusec();
			to_workers_set.clear();

			_reconf_tuples.push_back(0); //the first configuration is the starting one

		}

		void eosnotify(ssize_t id)
		{
			//We have to wake up sleeping workers (if any)
			//otherwise the collector will not receive all the EOS
			for(int i=_pardegree;i<_maximum_pardegree;i++)
			{
				_lb->thaw(i,true);
				while(!_lb->ff_send_out_to(EOS,i));
			}

			//send EOS to manager
			while(!_to_manager->push(new emitter_message_t(true)));
		}


		int svc_init()
		{
			//adjust the parallelism degree properly

			for(int i=_pardegree;i<_maximum_pardegree;i++)
				while(!_lb->ff_send_out_to(GO_OUT, i));
			return 0;
		}


		// destructor
		~WF_Emitter() {
			delete[] _to_workers;
		}

		// svc method
		tuple_wrapper_t* svc(in_tuple_t* in) {
			std::pair<int,long> info = _M_IN(*in);	// obtain from the tuple the id of the key and the id of the tuple
			unsigned int id_key = info.first;		// first is the id of the key the tuple belongs to
			unsigned long id_tuple = info.second;	// second is the id of the tuple
			id_tuple-=starting_id;						//adjust the tuple id

#if defined (SOFT_RECONFIGURATIONS)
			//If in a reconfiguration phase (change of par degree) checks if we reach the sliding switching point
			if(reconfiguring_pd && id_tuple==reconfiguration_switch_tuple_id)
			{
				//The reconfiguration phase has just finished
				//If we are removing some workers...
				if(new_num_worker<_pardegree)
				{
					//shut down the not needed workers
					for(int i=new_num_worker;i<_pardegree;i++)
						while(!_lb->ff_send_out_to(GO_OUT, i));
				}

				//in any case communicate to the remaining workers the new parellism degree (they have to adjust their slide)
				for(int i=0;i<new_num_worker;i++)
				{
					tuple_wrapper_t* punctuation =nullptr;
					_int_alloc_in->allocate_all((void*&)punctuation, 1);
					_int_alloc_in->construct(punctuation);
					punctuation->allocator = _int_alloc_in;
					punctuation->num_worker=new_num_worker;
					punctuation->batch_size=_batch_size; //we have to indicate it in case it has been modified in the meantime
					punctuation->tuple_id=id_tuple; //meaningless
					punctuation->key=id_key;
					punctuation->tag=PunctuationTag::CHANGE_PAR_DEGREE;
					while(!_lb->ff_send_out_to(punctuation,i));

				}
				reconfiguration_switch_tuple_id=-1;
				_pardegree=new_num_worker;
//				printf("[Emitter] Reconfiguration finished finita con tupla %d!Nuovo numero di worker: %d\n",id_tuple,_pardegree);
				reconfiguring_pd=false;

			}

#endif
			// determine the set of batches  the tuple belongs
			unsigned long first_id;
			if(id_tuple < _wb)
				first_id = 0;
			else
				first_id = floor(((double) (id_tuple - _wb))/((double) _sb))+1;

			unsigned long last_id = floor(((double) id_tuple)/((double) _sb));
			// determine the set of workers that receive the tuple in
			unsigned int countWorkers=0;
			unsigned long i = first_id;

			// the first window of key id_key is assigned to worker startWorkerId
			//NOTE: for the moment being reconfigurations works only with 1 key
			unsigned int startWorkerId = 0;


			if(!reconfiguring_pd)
			{
				//classical scheduling
				while((i<=last_id) && countWorkers<_pardegree) {
					_to_workers[countWorkers] = (startWorkerId + i)%_pardegree; // short transient phase with bounded queues
					countWorkers++;
					i++;
				}
			}
			else
			{
				while(i<=last_id && to_workers_set.size()<MAX(_pardegree,new_num_worker))
				{
					if(i<reconfiguration_last_id)
						to_workers_set.insert((startWorkerId + i)%_pardegree);
					else
						to_workers_set.insert((startWorkerId + i)%(new_num_worker));
					i++;
				}
			}

			// update the statistics for the number of copies per tuple
			_totalCopies+=countWorkers;
			_totalReceived++;


			// allocate and construct the wrapper
			tuple_wrapper_t* wrapper_in = nullptr;
			_int_alloc_in->allocate_all((void*&)wrapper_in, 1);
			_int_alloc_in->construct(wrapper_in);

			in_tuple_t* elem = (in_tuple_t*)((char *)wrapper_in + sizeof(tuple_wrapper_t));

			//_int_alloc_in->allocate(elem, 1);
			_int_alloc_in->construct(elem, *in); // THIS REQUIRES THAT in_tuple_t MUST HAVE A COPY CONSTRUTOR!!!!

			// set the wrapper parameters
			wrapper_in->counter = countWorkers;
			wrapper_in->item = elem;
			wrapper_in->allocator = _int_alloc_in;

			// for each destination we send the same wrapper of the input tuple

			if(!reconfiguring_pd )
			{
				for(int i = 0; i < countWorkers; i++) {
					while(!_lb->ff_send_out_to(wrapper_in, _to_workers[i]));
				}
			}
			else
			{
				for(int w:to_workers_set)
				{
					while(!_lb->ff_send_out_to(wrapper_in, w));
				}
				to_workers_set.clear();

			}


			//If in a reconfiguration due to change in the batch size, check if we reach the switching point
#if defined(SOFT_RECONFIGURATIONS) || defined (HARD_RECONFIGURATIONS)
			if(reconfiguring_bs && id_tuple>=reconfiguration_tuple_id )
			{
				tuples_bs.push_back(*in);

				if(id_tuple==reconfiguration_switch_tuple_id)
				{

#if defined(HARD_RECONFIGURATIONS)
					//if we are removing some workers, shut down them
					if(new_num_worker<_pardegree)
					{
						for(int i=new_num_worker;i<_pardegree;i++)
							while(!_lb->ff_send_out_to(GO_OUT, i));
					}
#endif

					//communicate to the workers that we reached the swithching point and they have to adjust their windows properly
#if defined(SOFT_RECONFIGURATIONS)
					for(int i=0;i<_pardegree;i++)
#elif defined(HARD_RECONFIGURATIONS)
					for(int i=0;i<new_num_worker;i++)
#endif
					{
						tuple_wrapper_t* punctuation =nullptr;
						_int_alloc_in->allocate_all((void*&)punctuation, 1);
						_int_alloc_in->construct(punctuation);
						punctuation->allocator = _int_alloc_in;
						punctuation->batch_size=new_batch_size;
#if defined(SOFT_RECONFIGURATIONS)
						punctuation->num_worker=_pardegree; //we have to communicate it in case it has been changed
						punctuation->tag=PunctuationTag::CHANGE_BATCH_SIZE;
#else
						punctuation->num_worker=new_num_worker;
						punctuation->tag=PunctuationTag::CHANGE_CONF;	//use a different puncutation just to differentiate if needed for other stuff
#endif
						punctuation->tuple_id=id_tuple; //meaningless
						punctuation->key=id_key;
						while(!_lb->ff_send_out_to(punctuation,i));

					}
					starting_id=reconfiguration_tuple_id+starting_id; //in case of successive modification we have to take into account also the previous ones

					reconfiguration_switch_tuple_id=-1;
					_batch_size=new_batch_size;
#if defined(HARD_RECONFIGURATIONS)
					_pardegree=new_num_worker;
#endif

					//recompute wb and sb
					_wb = _win_length + (_batch_size - 1) * _win_slide;
					_sb = _win_slide * _batch_size;
					//replay tuples
					for(int j=0;j<tuples_bs.size();j++)
					{
						std::pair<int,long> info = _M_IN(tuples_bs[j]);		// obtain from the tuple the id of the key and the id of the tuple
						unsigned int id_key = info.first;					// first is the id of the key the tuple belongs to
						unsigned long id_tuple = info.second;			// second is the id of the tuple
						id_tuple-=starting_id;
						if(id_tuple < _wb) first_id = 0;
						else first_id = floor(((double) (id_tuple - _wb))/((double) _sb))+1;
						long last_id = floor(((double) id_tuple)/((double) _sb));
						// determine the set of workers that receive the tuple in
						unsigned int countWorkers=0;
						unsigned long i = first_id;
						unsigned int startWorkerId=0;

						while((i<=last_id) && countWorkers<_pardegree) {
							_to_workers[countWorkers] = (startWorkerId + i)%_pardegree; // short transient phase with bounded queues
							countWorkers++;
							i++;
						}

						// allocate and construct the tuple
						tuple_wrapper_t* wrapper_in =nullptr;
						_int_alloc_in->allocate_all((void*&)wrapper_in, 1);
						_int_alloc_in->construct(wrapper_in);
						in_tuple_t* elem = (in_tuple_t*)((char *)wrapper_in + sizeof(tuple_wrapper_t));
						_int_alloc_in->construct(elem, tuples_bs[j]); // THIS REQUIRES THAT in_tuple_t MUST HAVE A COPY CONSTRUTOR!!!!

						// set the wrapper parameters
						wrapper_in->counter = countWorkers;
						wrapper_in->item = elem;
						wrapper_in->allocator = _int_alloc_in;

						for(int i = 0; i < countWorkers; i++) {
							while(!_lb->ff_send_out_to(wrapper_in, _to_workers[i]));
						}

					}


					reconfiguring_bs=false;
//					printf("[Emitter] Batch size changed to %d. Replicated %d tuples.\n",_batch_size,tuples_bs.size());
					tuples_bs.clear();
				}
			}
#endif


			// destroy and deallocate the original input element using the external allocator
			_ext_alloc_in->destroy(in);
			_ext_alloc_in->deallocate(in, 1);
#if defined (HARD_RECONFIGURATIONS)

			if(_from_manager->pop((void **)&_reconf_msg))
			{

				new_batch_size=_reconf_msg->new_batch_size;
				new_num_worker=_reconf_msg->new_par_degree;
				_start_rec_t=ff::getusec();

				//finish this scheduling round
				reconfiguration_tuple_id=(long int)((ceil((double)(id_tuple+1)/(_pardegree*_sb)))*_pardegree)*_sb;
				reconfiguration_last_id=floor(((double) reconfiguration_tuple_id)/((double) _sb));
				//compute the id of the tuple from which the reconfiguration has finished
				//and workers can change their slide accordingly
				reconfiguration_switch_tuple_id=reconfiguration_tuple_id+_wb-_sb-1;
				reconfiguring_bs=true;
				_reconf_tuples.push_back(reconfiguration_tuple_id-id_tuple);

				//if we are adding workers we have to wake up them
				if(new_num_worker>_pardegree)
				{
					//wake up
					for(int i=_pardegree;i<new_num_worker;i++)
					{
						_lb->thaw(i,true);
					}
				}
				delete _reconf_msg;

			}


#elif defined(SOFT_RECONFIGURATIONS)

			//check if the manager has sent some reconfiguration message
			if(_from_manager->pop((void **)&_reconf_msg))
			{
				//PER CAMBIARE CONFIGURAZIONE PIU' VELOCEMENTE: vedi appunti in Win_MC_WF.hpp

				printf("[Emitter] Received reconfiguration message: %d, %d (prev %d %d)\n",_reconf_msg->new_par_degree,_reconf_msg->new_batch_size,_pardegree,_batch_size);
				//recognize the type of required reconfiguration

				_start_rec_t=ff::getusec();
				//ATTENZIONE: va gestito il caso di doppia riconfigurazione
				if(_reconf_msg->new_par_degree!=_pardegree)
				{
					//we have to change the degree of parallelism
					new_num_worker=_reconf_msg->new_par_degree;
					int mcm=lcm(new_num_worker,_pardegree);
					//compute the id of the first tuple (and the related last) from which we can switch
					//to the new scheduling. It will be the first tuple of the first window assigned to the first worker
					//either according to the previous number of workers or to the new one
					reconfiguration_tuple_id=(long int)((ceil((double)(id_tuple+1)/(mcm*_sb)))*mcm)*_sb;
					//compute the last_id of the tuple
					reconfiguration_last_id=floor(((double) reconfiguration_tuple_id)/((double) _sb));
					//compute the id of the tuple from which the reconfiguration has finished
					//and workers can change their slide accordingly
					reconfiguration_switch_tuple_id=reconfiguration_tuple_id+_wb/*-sb*new_num_worker*/;

					//profiling
					_reconf_tuples.push_back(reconfiguration_tuple_id-id_tuple);

					reconfiguring_pd=true;
					//if we are adding workers we have to wake up them
					if(new_num_worker>_pardegree)
					{
						//wake up
						for(int i=_pardegree;i<new_num_worker;i++)
						{
							_lb->thaw(i,true);
							tuple_wrapper_t* punctuation =nullptr;
							_int_alloc_in->allocate_all((void*&)punctuation, 1);
							_int_alloc_in->construct(punctuation);
							punctuation->allocator = _int_alloc_in;
							punctuation->batch_size=_batch_size;
							punctuation->num_worker=new_num_worker; //we have to communicate it in case it has been changed
							punctuation->tuple_id=id_tuple; //meaningless
							punctuation->key=id_key;
							punctuation->tag=PunctuationTag::CHANGE_PAR_DEGREE;
							while(!_lb->ff_send_out_to(punctuation,i));
						}
					}
				}
				else
				{
					if(_reconf_msg->new_batch_size!=_batch_size)
					{
						new_batch_size=_reconf_msg->new_batch_size;
						int mcm=lcm(_win_slide*_batch_size,_win_slide*new_batch_size);

						reconfiguration_tuple_id=(long int)((ceil((double)(id_tuple+1)/(mcm*_pardegree)))*_pardegree)*mcm;
						//questa la calcoliamo considerando il vecchio bsize
						reconfiguration_last_id=floor(((double) reconfiguration_tuple_id)/((double) _sb));
						//questa secondo il nuovo

						reconfiguration_switch_tuple_id=reconfiguration_tuple_id+_wb-_sb-1;
						//profiling
						_reconf_tuples.push_back(reconfiguration_tuple_id-id_tuple);

						reconfiguring_bs=true;
//						printf("[Emitter] ricevuto cambio bsize con tupla %Ld. Switch scheduling settato a %Ld (last: %Ld) (# of tuples: %d). Switching tuple: %Ld\n",id_tuple,reconfiguration_tuple_id,reconfiguration_last_id,_reconf_tuples.back(),reconfiguration_switch_tuple_id);
					}
				}
				delete _reconf_msg;
			}

#endif

			return ff_node_t<in_tuple_t, tuple_wrapper_t>::GO_ON;
		}

		// svc_end method
		void svc_end() {
			//print statistics from the emitter view point (i.e. tuples needed to start the new configuration)
			FILE *fout=fopen("interpolation_em.dat","w");
			fprintf(fout,"#Numb of tuples\n");
			for(int t:_reconf_tuples)
				fprintf(fout,"%d\n",t);
			fclose(fout);
		}
	};

	/*!
	*  \internal WF_Collector
	*
	*  \brief The Collector of the Window GPU Farming Pattern
	*
	*  This is the Collector of the Window Farming pattern that collects results from the
	*  workers and produces an ordered sequence of results outside. A partial ordering is
	*  applied: all the results with the same key are ordered but a non-deterministic order
	*  is followed by the results belonging to different keys.
	*
	*  This inner class is defined in \ref HLPP_patterns/include/Win_WF.hpp
	*/
	class WF_Collector: public ff_node_t<output_wrapper_t, out_tuple_t> {
	private:
		FUNC_OUT_T _M_OUT; // function to retrieve the key id and result id from an output result
		// inner class for the comparisor functor used in the priority queue
		class comparisor {
			FUNC_OUT_T func;
		public:
			comparisor(FUNC_OUT_T _func):func(_func) {}
			bool operator() (const output_wrapper_t a, const output_wrapper_t b) const {
				unsigned long a_id = func(*(a.item)).second;
				unsigned long b_id = func(*(b.item)).second;
				return a_id > b_id;
			}
		};
		std::vector<std::unique_ptr<std::priority_queue<output_wrapper_t, std::vector<output_wrapper_t>, comparisor>>> queueVector;
		std::vector<unsigned int> nextidVector;
		Ext_Alloc_OUT_T* ext_alloc_out; // pointer to the external allocator of output results
		bool isOrdered; // true if the collector is ordered (partial) false otherwise
		unsigned int resize_factor; // resize factor used by the internal vectors (queueVector and nextidVector)

		//for communication with the manager
		SWSR_Ptr_Buffer *_to_manager;
		//we have to measure the produce throughput
		int _forwarded_last_second=0;
		long _start_t;
		bool _send_to_manager=true;
		std::vector<double> _throughput_per_second;			//to keep track of the received throughput (for the moment being it is just for monitoring not really used)
		std::vector<double> _throughput_after_reconf;		//after a reconfiguration we have to monitor the throughput to understand if it is stable or not
		std::vector<double> _coeffs_var_after_reconf;		//we monitor also the coeffient of variations throughout time

	public:
		// constructor
		WF_Collector(FUNC_OUT_T M_OUT, Ext_Alloc_OUT_T* ext_alloc_out, bool isOrdered, SWSR_Ptr_Buffer *to_manager=nullptr,unsigned int resize_factor=3000)
			:_M_OUT(M_OUT), queueVector(resize_factor), nextidVector(resize_factor), ext_alloc_out(ext_alloc_out),
			  isOrdered(isOrdered), resize_factor(resize_factor), _to_manager(to_manager) {
			_throughput_after_reconf.reserve(_mov_average_length+1);
			_coeffs_var_after_reconf.reserve(_cv_average_length+1);
			_throughput_per_second.reserve(500);
			//_send_to_manager=true;			//we send to the manager the throughput obtained with the first configuration (1,384)

		}



		// svc_init method
		int svc_init() {
			// initialization of the internal vectors
			for(int i = 0; i < resize_factor; i++) {
				queueVector[i] = nullptr; // the queue is unitialized
				nextidVector[i] = 0;
			}
			_start_t=ff::getusec();
			return 0;
		}


		// svc method
		out_tuple_t* svc(output_wrapper_t* wrapper_out) {

			if(wrapper_out->tag==PunctuationTag::NONE)
			{

				out_tuple_t* result = wrapper_out->item;
				std::pair<unsigned int, unsigned long> result_info = _M_OUT(*result);

				unsigned int key = result_info.first; // extract the key
				unsigned long id = result_info.second; // extract the id of the result
				int forwarded_elements=0;	    //number of elements that are actually sent out from the collector (considering also the buffering for the order)
				if(!isOrdered) { // the collector is not ordered

					// allocate and construct the to_send result with the external allocator
					out_tuple_t* to_send = ext_alloc_out->allocate(1);
					ext_alloc_out->construct(to_send, *result); // THIS REQUIRES THAT out_tuple_t MUST HAVE A COPY CONSTRUTOR!!!!
					// destroy and deallocate the result with the allocator in the wrapper
					Win_Allocator<out_tuple_t>* allocator = wrapper_out->allocator;
					allocator->destroy(result);
					allocator->deallocate(result, 1);
					// destroy and deallocate the wrapper with the allocator in the wrapper
					allocator->destroy(wrapper_out);
					allocator->deallocate(wrapper_out, 1);
					forwarded_elements++;
					while(!ff_node_t<output_wrapper_t, out_tuple_t>::ff_send_out(to_send));
				}
				else { // the collector is ordered

					// check if the key is in the actual vectors, otherwise resize them
					if (key >= queueVector.size()) {
						unsigned int old_size = queueVector.size();
						// resize the two vectors
						queueVector.resize(key+1);
						nextidVector.resize(key+1);
						// initialize the new elements of the two vectors
						for(int i = old_size; i < key+1; i++) {
							queueVector[i] = nullptr; // the queue is unitialized
							nextidVector[i] = 1;
						}
					}
					// check if the queue of the key is initiliazed, otherwise initialize it
					if(queueVector[key] == nullptr) {
						std::unique_ptr<std::priority_queue<output_wrapper_t, std::vector<output_wrapper_t>, comparisor>> q(new std::priority_queue<output_wrapper_t, std::vector<output_wrapper_t>, comparisor>(comparisor(_M_OUT)));
						queueVector[key] = std::move(q);
					}
					//if(nextidVector[key-1]>id)
					//	printf("-------Ricevuto duplicato %ld\n",id);
					// if next_id == id then the element can be safely transmitted
					forwarded_elements++;
					if(nextidVector[key] == id) {

						// allocate and construct the to_send result with the external allocator
						out_tuple_t* to_send = ext_alloc_out->allocate(1);
						ext_alloc_out->construct(to_send, *result); // THIS REQUIRES THAT out_tuple_t MUST HAVE A COPY CONSTRUTOR!!!!
						// destroy and deallocate the result with the allocator in the wrapper
						Win_Allocator<out_tuple_t>* allocator = wrapper_out->allocator;
						allocator->destroy(result);
						allocator->deallocate(result, 1);
						// destroy and deallocate the wrapper with the allocator in the wrapper
						allocator->destroy(wrapper_out);
						allocator->deallocate(wrapper_out, 1);

						//forwarded_elements++;
						while(!ff_node_t<output_wrapper_t, out_tuple_t>::ff_send_out(to_send));
						// increment next_id
						nextidVector[key]++;
						// if the queue is not empty, dequeue the priority queue from results with contiguous ids starting from next_id
						if(!queueVector[key]->empty()) {
							output_wrapper_t wrapper_next = queueVector[key]->top();
							out_tuple_t* next = wrapper_next.item;
							while(_M_OUT(*next).second == nextidVector[key]) {
								queueVector[key]->pop(); // remove the output wrapper from the queue
								// allocate and construct the to_send result with the external allocator
								out_tuple_t* to_send = ext_alloc_out->allocate(1);
								ext_alloc_out->construct(to_send, *next); // THIS REQUIRES THAT out_tuple_t MUST HAVE A COPY CONSTRUTOR!!!!
								// destroy and deallocate the next result with the allocator in the wrapper
								Win_Allocator<out_tuple_t>* allocator = wrapper_next.allocator;
								allocator->destroy(next);
								allocator->deallocate(next, 1);

								//forwarded_elements++;
								while(!ff_node_t<output_wrapper_t, out_tuple_t>::ff_send_out(to_send));
								// increment next_id
								nextidVector[key]++;
								if(queueVector[key]->empty()) break; // if queue is now empty break
								// extract the next top output descriptor from the queue
								wrapper_next = queueVector[key]->top();
								next = wrapper_next.item;
							}
						}
					}
					// otherwise the element must be buffered
					else {
//						std::cout << "expected id "<<nextidVector[key]<< " while received "<< id <<std::endl;
						// insert the wrapper in the priority queue (by copying it)
						queueVector[key]->push(*wrapper_out);
						// destroy and deallocate the wrapper with the allocator in the wrapper
						Win_Allocator<out_tuple_t>* allocator = wrapper_out->allocator;
						allocator->destroy(wrapper_out);
						allocator->deallocate(wrapper_out, 1);
					}
				}

				//in every case we received a new result (HERE we consider the effect of ordering)
				//TODO: QUESTA COSA PUO' essere fonte di problemi
				_forwarded_last_second+=forwarded_elements;
				if(getusec()-_start_t>(long)_monitoring_interval && _forwarded_last_second>0)
				{

//					printf("Start time: %Ld End time %Ld  Num: %d \n",_start_t,getusec(),_forwarded_last_second);
					//we have to take into account the real time elapsed (it may be higher than one second)
					_throughput_per_second.push_back((double)_forwarded_last_second/((double)(getusec()-_start_t)/1000000.0));
					//		    printf("Throughput collettore= %d\n",_throughput_per_second.back());
					if(_send_to_manager)
					{
						//check whether the throughput is stable
						//i.e. if the coefficient of variation is below 0.1
						//as additional possibility we take into account the coefficient of variation
						//of the coefficients of variation: if it is below 0.1 we are OK
						//This because we can have situations in which the rate naturally fluctuates
						//over time: in this way we know that this "fluctuation is stable"
						//This will require at leat _mov_average_length+_cv_average_length seconds
						//to be triggered

						_throughput_after_reconf.push_back(_throughput_per_second.back());

						if(_throughput_after_reconf.size()>_mov_average_length)
							_throughput_after_reconf.erase(_throughput_after_reconf.begin());

						if(_throughput_after_reconf.size()==_mov_average_length)
						{
							double mean = 0, stdev=0;

							for(double t:_throughput_after_reconf)
								mean+=t;

							mean/=_throughput_after_reconf.size();

							for(double t:_throughput_after_reconf)
								stdev+=(t-mean)*(t-mean);
							stdev=sqrt(stdev/(_throughput_after_reconf.size()-1));//corrected sampled standard deviation
//							for(double t:_throughput_after_reconf)
//								printf("%.1f ",t);
//							printf("Stdev/mean=%.2f\n",stdev/mean);

							if(stdev/mean<=THR_STABLE_CV)
							{
								//if throughput stable enough we can send it to the manager
								while(!_to_manager->push(new collector_message_t(mean)));

//								printf("[Collector] We have a stable throughput: %.1f\n",mean);
//								for(int t:_throughput_after_reconf)
//									printf("%d ",t);
//								printf("\n");
								_throughput_after_reconf.clear();
								_coeffs_var_after_reconf.clear();
								_send_to_manager=false;

							}
							else
							{
								//save the coeff of variation for further analysis
								_coeffs_var_after_reconf.push_back(stdev/mean);

								if(_coeffs_var_after_reconf.size()>_cv_average_length)
									_coeffs_var_after_reconf.erase(_coeffs_var_after_reconf.begin());
								if(_coeffs_var_after_reconf.size()==_cv_average_length)
								{
									double mean_cv=0, stdev_cv=0;

									for(double d:_coeffs_var_after_reconf)
										mean_cv+=d;

									mean_cv/=_cv_average_length;

									for(double d:_coeffs_var_after_reconf)
										stdev_cv+=(d-mean_cv)*(d-mean_cv);

									stdev_cv=sqrt(stdev_cv/(_cv_average_length-1)); //corrected standard deviation

//									printf("-->Coefficient of variaations of CsV: %.2f\n",stdev_cv/mean_cv);

									if(stdev_cv/mean_cv<=0.15)
									{
										while(!_to_manager->push(new collector_message_t(mean)));

//										printf("[Collector] We have a stable throughput with CV: %.1f\n",mean);
//										for(int t:_throughput_after_reconf)
//											printf("%d ",t);
//										printf("\n");
										_throughput_after_reconf.clear();
										_coeffs_var_after_reconf.clear();
										_send_to_manager=false;
									}



								}
							}
						}


					}
					_start_t=getusec();
					_forwarded_last_second=0;
				}
			}
			else
			{
				//only the first worker will send the notification to the collector if a reconfiguration has been triggered
				printf("[Collector] received notification from the worker\n");
				//now we have to wait till the throughput is stable....
				_send_to_manager=true;
				//manca tutta la parte di statistche e questo non va qua
			}
			return ff_node_t<output_wrapper_t, out_tuple_t>::GO_ON;
		}


	};

	/**
	 * \brief constructor I
	 *
	 * \param _M_IN the function to extract the key value and tuple id from the input tuple
	 * \param _M_OUT the function to extract the key value and tuple id from the output tuple
	 * \param cuda_fun the processing function of the Workers over the GPU
	 * \param _nw the actual parallelism degree
	 * \param _wlen the length of the windows
	 * \param _wslide the sliding factor of the windows
	 * \param _ext_alloc_in reference to an external allocator of input tuples (if not specified the type must be a stateless allocator)
	 * \param _ext_alloc_out reference to an external allocator of output results (if not specified the type must be a stateless allocator)
	 * \param input_ch states the presence of input channel
	 * \param batch_size size of the bath in windows
	 * \param ordering flag to disable partial ordering in the collector
	 *
	 *
	 * PLEASE NOTE: for the moment being we suppose that it receives an indication of the incomin rate (expressed as producible results per second)
	 *				Possibly, it will be no more needed
	 *
	 *				It is better to run with Bounded queues (in order to not have a long reconfiguration phase)
	 *
	 */
	Win_GPU_WF(FUNC_IN_T M_IN, FUNC_OUT_T M_OUT, CUDA_F_T cuda_fun, unsigned int nw, unsigned long wlen, unsigned long wslide,
			   Ext_Alloc_IN_T& ext_alloc_in, Ext_Alloc_OUT_T& ext_alloc_out,
			   unsigned int batch_size, int rate=0, size_t scratchpad_size=0)
#if defined(BOUNDED_QUEUE)
	//in case of bounded queue we must assure that the emitter will not block while sending an entire batch to a worker
	//therefore queues must be at least equal to wlen+(b-1)*ws for each worker. We added a little bit of extra space
	//by default we use BOUNDED_QUEUE to ensure fixed amount length of queue and hence limited amount of time required to reconfigure
		:ff_farm<>(false, (wlen+wslide*MAX_BATCH_SIZE+2048)*2*MAX_NUM_WORKERS, 5000*MAX_NUM_WORKERS, true, MAX_NUM_WORKERS, true), _pardegree(nw),
		  _ext_alloc_in(&ext_alloc_in), _ext_alloc_out(&ext_alloc_out),_rate(rate)
	#else
		:ff_farm<>(false, DEF_IN_BUFF_ENTRIES, DEF_OUT_BUFF_ENTRIES, true, MAX_NUM_WORKERS), _pardegree(nw),
		  _ext_alloc_in(&ext_alloc_in), _ext_alloc_out(&ext_alloc_out),_rate(rate)

	#endif
	{

#if defined(HARD_RECONFIGURATIONS) || defined (SOFT_RECONFIGURATIONS)
#if defined(SOFT_RECONFIGURATIONS)
		printf("**************************   SOFT RECONFIGURATIONS   ********************************\n");
#else
		printf("**************************   HARD RECONFIGURATIONS   ********************************\n");
#endif
		//comment these two lines if you are testing the mechanisms
		_pardegree=_starting_pardegree;
		batch_size=_starting_bs;
#endif

		std::cout << "Kernel will be executed by "<< ceil((double)batch_size/MAX_CUDA_THREADS_PER_BLOCK) << " blocks of "<< MAX_CUDA_THREADS_PER_BLOCK << " threads"<<std::endl;

		assert(_pardegree>0);
		assert(wslide <= wlen); // we support sliding or tumbling windows (not hopping windows)!
		assert(PER_WORKER_OUTPUT_QUEUE_LEN>=batch_size); //otherwise worker may be blocked


		std::vector<ff_node *> w(MAX_NUM_WORKERS);
		Win_Allocator<out_tuple_t> * out_allocator = nullptr;

		initialize_internals(out_allocator);

		for(size_t i = 0; i<MAX_NUM_WORKERS; i++) {
			w[i] = new Win_Seq_Wrapper<in_tuple_t, out_tuple_t, win_type_t, CUDA_F_T>(M_IN, cuda_fun, wlen, wslide, _cuda_streams[i],
																		batch_size,MAX_BATCH_SIZE,_pardegree,overlap_accumulation,
																		out_allocator,_from_workers_to_manager[i],scratchpad_size);
		}
		ff_farm<>::add_workers(w); // add workers
		ff_farm<>::add_collector(new WF_Collector(M_OUT, _ext_alloc_out, ordering,_from_collector_to_manager)); // add customized collector
		ff_farm<>::add_emitter(new WF_Emitter(M_IN, wlen, wslide, batch_size, _pardegree, this->getlb(), _ext_alloc_in,allocator_kind,_from_emitter_to_manager,_from_manager_to_emitter)); // add customized emitter

		//create the manager
		manager=new Win_GPU_Manager(_from_emitter_to_manager,_from_manager_to_emitter,_from_workers_to_manager,_from_collector_to_manager,_pardegree,batch_size,_rate);
	}

	/**
	 * \brief constructor II - Pane based version
	 *
	 * \param _M_IN the function to extract the key value and tuple id from the input tuple
	 * \param _M_OUT the function to extract the key value and tuple id from the output tuple
	 * \param cuda_fun the Pane processing function of the Workers over the GPU.
	 * \param h_fun the second level aggregation Function that will be executed aver the pane one
	 * \param _nw the actual parallelism degree
	 * \param _wlen the length of the windows
	 * \param _wslide the sliding factor of the windows
	 * \param _ext_alloc_in reference to an external allocator of input tuples (if not specified the type must be a stateless allocator)
	 * \param _ext_alloc_out reference to an external allocator of output results (if not specified the type must be a stateless allocator)
	 * \param input_ch states the presence of input channel
	 * \param batch_size size of the bath in windows
	 * \param ordering flag to disable partial ordering in the collector
	 *
	 *
	 * PLEASE NOTE: for the moment being we suppose that it receives an indication of the incomin rate (expressed as producible results per second)
	 *				Possibly, it will be no more needed
	 *
	 *				It is better to run with Bounded queues (in order to not have a long reconfiguration phase)
	 *
	 */
	Win_GPU_WF(FUNC_IN_T M_IN, FUNC_OUT_T M_OUT, CUDA_F_T cuda_fun, FUNC_H_T h_fun,  unsigned int nw, unsigned long wlen, unsigned long wslide,
			   Ext_Alloc_IN_T& ext_alloc_in, Ext_Alloc_OUT_T& ext_alloc_out,
			   unsigned int batch_size, int rate=0, size_t scratchpad_size=0)
		:ff_farm<>(false, (wlen+wslide*MAX_BATCH_SIZE+2048)*2*MAX_NUM_WORKERS, 5000*MAX_NUM_WORKERS, true, MAX_NUM_WORKERS, true), _pardegree(nw),
		  _ext_alloc_in(&ext_alloc_in), _ext_alloc_out(&ext_alloc_out),_rate(rate)
	{
		assert(_pardegree>0);
		assert(wslide <= wlen); // we support sliding or tumbling windows (not hopping windows)!
		assert(PER_WORKER_OUTPUT_QUEUE_LEN>=batch_size); //otherwise worker may be blocked
		std::cout << "Pane size: "<<gcd(wlen,wslide)<<std::endl;

		std::vector<ff_node *> w(MAX_NUM_WORKERS);
		Win_Allocator<out_tuple_t> * out_allocator = nullptr;

		initialize_internals(out_allocator);

		for(size_t i = 0; i<MAX_NUM_WORKERS; i++) {
			w[i] = new Win_Seq_Wrapper<in_tuple_t, out_tuple_t, win_type_t, CUDA_F_T>(M_IN, cuda_fun, h_fun, wlen, wslide, _cuda_streams[i],
																		batch_size,MAX_BATCH_SIZE,_pardegree,overlap_accumulation,
																		out_allocator,_from_workers_to_manager[i],scratchpad_size);
		}
		ff_farm<>::add_workers(w); // add workers
		ff_farm<>::add_collector(new WF_Collector(M_OUT, _ext_alloc_out, ordering,_from_collector_to_manager)); // add customized collector
		ff_farm<>::add_emitter(new WF_Emitter(M_IN, wlen, wslide, batch_size, _pardegree, this->getlb(), _ext_alloc_in,allocator_kind,_from_emitter_to_manager,_from_manager_to_emitter)); // add customized emitter

		//create the manager
		manager=new Win_GPU_Manager(_from_emitter_to_manager,_from_manager_to_emitter,_from_workers_to_manager,_from_collector_to_manager,_pardegree,batch_size,_rate);

	}




	// destructor
	~Win_GPU_WF() {
		if(freeAlloc) {
			delete _ext_alloc_in;
			delete _ext_alloc_out;
		}
	}

	int run(bool skip_init=false)
	{
#if defined(HARD_RECONFIGURATIONS) || defined (SOFT_RECONFIGURATIONS)
		//start the manager
		manager->start();
		//TODO: capire perche' con affinita' va peggio
#endif
		ff_farm<>::run(skip_init);
	}

	int wait()
	{
		ff_farm<>::wait();
#if defined(HARD_RECONFIGURATIONS) || defined (SOFT_RECONFIGURATIONS)
		//manager->join(); //join also the manager
		delete manager;
#endif
	}


	// method to enable batching and set the batch length in number of tuples
	//NOTE: no more used: prone do be deleted
	virtual void enable_batch(unsigned long length) {
		// return the svector of the workers
		const svector<ff_node*>& workers = this->getWorkers();
		// we enable the batch in all the inner patterns
		for(size_t i = 0; i<_pardegree; i++) (static_cast<Win_Seq_Wrapper<in_tuple_t, out_tuple_t, win_type_t,CUDA_F_T> *const>(workers[i]))->enable_batch(length);
	}

	// method to disable batching
	//NOTE: no more used: prone do be deleted
	virtual void disable_batch() {
		// return the svector of the workers
		const svector<ff_node*>& workers = this->getWorkers();
		// we enable the batch in all the inner patterns
		for(size_t i = 0; i<_pardegree; i++) (static_cast<Win_Seq_Wrapper<in_tuple_t, out_tuple_t, win_type_t, CUDA_F_T> *const>(workers[i]))->disable_batch();
	}

private:

	/**
	 * @brief initialize_queues build queues to manager, allocator, cuda streams
	 */
	void initialize_internals(Win_Allocator<out_tuple_t> * &out_allocator)
	{
		/** Instantiate the queues for the Manager*/
		_from_emitter_to_manager=new SWSR_Ptr_Buffer(64);
		_from_emitter_to_manager->init();
		_from_manager_to_emitter=new SWSR_Ptr_Buffer(64);
		_from_manager_to_emitter->init();
		_from_collector_to_manager=new SWSR_Ptr_Buffer(64);
		_from_collector_to_manager->init();
		for(int i=0;i<MAX_NUM_WORKERS;i++)
		{
			_from_workers_to_manager.push_back(new SWSR_Ptr_Buffer(64));
			_from_workers_to_manager[i]->init();
		}
		_cuda_streams.reserve(MAX_NUM_WORKERS);
		for(size_t i = 0; i<MAX_NUM_WORKERS; i++)
		{
			//CUDA create the stream
			if(cudaStreamCreate(&_cuda_streams[i])!=cudaSuccess)
				std::cerr<< "Error in creating CUDA streams!"<<std::endl;
		}


		if (allocator_kind == 0) {// standard
			printf("WORKERS Using the standard allocator\n");
			out_allocator = new Win_Allocator<out_tuple_t>();
		} else if (allocator_kind == 1) { // TBB
			printf("WORKERS Using the TBB allocator\n");
			abort(); //not supported now
			//			out_allocator = new Win_AllocatorTBB<out_tuple_t>();
		} else if (allocator_kind == 2) { // FF
			printf("WORKERS CANNOT USE FF allocator, SWITCHING TO STATIC, (magic=%ld)\n", (PER_WORKER_OUTPUT_QUEUE_LEN * 2 * _pardegree));
			out_allocator = new Win_StaticAllocator<out_tuple_t>(PER_WORKER_OUTPUT_QUEUE_LEN * 2 * _pardegree);
		} else {
			printf("WORKERS Using the STATIC allocator (magic=%ld)\n", (PER_WORKER_OUTPUT_QUEUE_LEN * 2 * _pardegree));
			out_allocator = new Win_StaticAllocator<out_tuple_t>(PER_WORKER_OUTPUT_QUEUE_LEN * 2 * _pardegree);
		}

	}

};

#endif
