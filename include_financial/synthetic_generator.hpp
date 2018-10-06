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


  Implementation of the synthetic-generator as ff_node
  in order to be used directly in a FF pipeline

  For the moment being it does not accept file with variable rates
  TODO: implement this feature

  NOTE: it pregenerates all the data, so the memory occupancy of the application will increase

  */
#ifndef SYNTHETICGENERATOR_HPP
#define SYNTHETICGENERATOR_HPP
#include <ff/node.hpp>
#include <pthread.h>
#include <stdlib.h>
#include "random_generator.h"
#include "general.h"

#define EXPONENTIAL
using namespace ff;
class SyntheticGenerator: public ff_node_t<tuple_t>
{
public:
	SyntheticGenerator(int num_keys, double rate, long num_tuples):
		_num_keys(num_keys), _rate(rate),_num_tuples(num_tuples),_generator(1)
	{
		//readDistributions(distribution_file,&_times, &_probability_distribution);
		_probability_distribution[0][0]=1.0;
		srand(10);

#if defined(PREGENERATE)
		//pregenerate all the data to save time
		_tuples=new tuple_t[_num_tuples]();
		for(int i=0;i<_num_tuples;i++)
		{
			_tuples[i].id=i;
			_tuples[i].type=generateRandomClass(_probability_distribution[0],_num_keys);
			// printf("Invio classe: %d\n",task.type);
			_tuples[i].bid_price=_generator.uniform(100, 200);
			_tuples[i].bid_size=_generator.uniform(0, 200);
			_tuples[i].ask_price=_generator.uniform(100, 200);
			_tuples[i].ask_size=_generator.uniform(0, 200);
		}
		printf("Pregenerated tuples!\n");
#else
		printf("Tuples generated on the fly!\n");
#endif
	}

	~SyntheticGenerator()
	{
		free(_sums);
#if defined(PREGENERATE)
		delete[] _tuples;
#endif

	}


	// svc method
	tuple_t* svc(tuple_t*) {

		printf("Start generating tuples ");
#if defined(EXPONENTIAL)
		printf("with exponential distribution\n");
#elif defined(NORMAL)
		printf("with normal distribution (nu=%.3f, sigma=%.3f)\n",((double)1000000000)/_rate , ((double)1000000000)/_rate/10);
#else
		printf("with fixed distribution\n");
#endif
		//first tuple?
		double waitingTime=(((double)1000000000)/(_rate)); //expressed in nano secs
		long start_t=current_time_usecs();
		long start_time=current_time_nsecs();
		long last_monitoring_time=start_t;
		double next_send_time_nsecs=0;          //the time (in nanosec) at which the next tuple has to be sent
		long last_send_time_nsec=current_time_nsecs();	//the time the next tuple has been sent (used for monitoring purposes, e.g. interdep. times)
		int tuples_sent_last_step=0;				//monitoring the rate
#if !defined(PREGENERATE)
		tuple_t *task;
#endif
		for(long i=0;i<_num_tuples;i++)
		{
#if !defined(PREGENERATE)
			task=new tuple_t();
			task->id=i;
			task->type=generateRandomClass(_probability_distribution[0],_num_keys);
		    // printf("Invio classe: %d\n",task.type);
			task->bid_price=_generator.uniform(100, 200);
//			task->bid_price=1;
			task->bid_size=_generator.uniform(0, 200);
			task->ask_price=_generator.uniform(100, 200);
//			task->ask_price=1;
			task->ask_size=_generator.uniform(0, 200);
#endif
			//TODO: handle multiple probability distribution and rates
			volatile long end_wait=start_time+next_send_time_nsecs;
			volatile long curr_t=current_time_nsecs();
			while(curr_t<end_wait)
				curr_t=current_time_nsecs();

#if defined(PREGENERATE)

			_tuples[i].timestamp=(long)(next_send_time_nsecs/1000.0);  //the timestamp is in usec basis
			_tuples[i].original_timestamp=((int)(_tuples[i].timestamp/1000))*1000;
			while(!ff_send_out(&_tuples[i]));
#else
			task->timestamp=(long)(next_send_time_nsecs/1000.0);  //the timestamp is in usec basis
			task->original_timestamp=((int)(task->timestamp/1000))*1000;
			while(!ff_send_out(task));
#endif

			if(i%_monitoring_every_x_tuples==0) //save the actual interdep. time
			{
				_monitoring_interdep_times.push_back(curr_t-last_send_time_nsec);
				asm volatile ("":::"memory");
			}
#if defined(EXPONENTIAL)
			//rate with exponential distribution
			next_send_time_nsecs+=_generator.expntl(((double)1000000000)/(_rate));
#elif defined(NORMAL)
			next_send_time_nsecs+=_generator.normal(((double)1000000000)/_rate , ((double)1000000000)/_rate/10);

#else
			//fixed rate
			next_send_time_nsecs+=waitingTime;
#endif
			tuples_sent_last_step++;
			//once in a while record the current rate
			last_send_time_nsec=curr_t;
			asm volatile ("":::"memory");
		}
		printf("End generator. Rate generator: %f \n",_num_tuples/((current_time_usecs()-start_t)/1000000.0));

		/*
		 * This is if we want to record the rate...otherwise we can plot the departure time of each tuple
		 * FILE *fgenerator=fopen("generator_rates.dat","w");
		fprintf(fgenerator, "#TIME\tRATE(T\\s)\n");
		for(int i=0;i<_monitoring_times.size();i++)
		{
			fprintf(fgenerator,"%3.3f\t%.3f\n",_monitoring_times[i],_monitoring_rates[i]);
		}

		fclose(fgenerator);*/
#if defined(PREGENERATE)
		// Print the interdeparture time of the tuples. We want to avoid to print millions of values.
		// For this reason we will print the departure time of the tuple every X tuples

		FILE *ftuple=fopen("tuples_interdep_times.dat","w");
		int print_every_x_tuples=100;
		fprintf(ftuple,"#Interdeparture time from the last tuple\n");
		fprintf(ftuple,"%ld\n",_tuples[0].timestamp);
		for(long it:_monitoring_interdep_times)
		{

			fprintf(ftuple,"%ld\n",it);

		}
		fclose(ftuple);
#endif
		return EOS;
	}

private:
	double _probability_distribution[1][1]; //in this case we have only one probability distribution (one key)
	int _num_keys;
	long _num_tuples;
	double _rate;
	tuple_t *_tuples;
	double *_sums=NULL; //used for data generation (keys)
	RandomGenerator _generator;
	std::vector<double> _monitoring_times;
	std::vector<double> _monitoring_rates;
	std::vector<long> _monitoring_interdep_times;	//in nanosec
	int _monitoring_every_x_tuples=1024;			//record the interdep. every x tuples




	/*
		Reads the distribution from the file passed as argument
		File format:
			<num_class> <num_distributions>
			<time_0> <vect_0>
			...
		where times are expressed in second and represent the time from which the relative distribution (expressed with the vector
		of probability) is valid.
		The first time is assumed to be 0.

		Return the number of distribution.
		The pointer to the vector of times and the pointer to the matrix of distributions are reported in the
		the parameters times and distributions
	*/
	int readDistributions(char *file, int **times, double ***distributions)
	{
		//this is not used since we have only one key
		FILE *fdistr=fopen(file,"r");
		srand(10);
		int nclass;
		int ndistributions;
		int i,j;
		if(!fscanf(fdistr,"%d %d\n",&nclass,&ndistributions))
			printf("Error in reading the file!");
		//allocate the space
		int *t=(int *)malloc(sizeof(int)*nclass);
		double **d=(double **)malloc(sizeof(double *)*ndistributions);
		for(i=0;i<ndistributions;i++)
			d[i]=(double *)malloc(sizeof(double)*nclass);

		//start reading
		int err;
		for(i=0;i<ndistributions;i++)
		{
			err=fscanf(fdistr, "%d",&t[i]);
			for(j=0;j<nclass;j++)
			{
				err=fscanf(fdistr,"%lf",&d[i][j]);
				// printf("%3.5f ",d[i][j]);
			}

		}
		// printf("\n");
		*times=t;
		*distributions=d;
		return ndistributions;

	}

	int bsearch(double AR[], int N, double VAL)
	{
		int Mid,Lbound=0,Ubound=N-1;

		while(Lbound<=Ubound)
		{
			Mid=(Lbound+Ubound)/2;
			if(VAL>AR[Mid])
				Lbound=Mid+1;
			else if(VAL<AR[Mid])
				Ubound=Mid-1;
			else
				return Mid;
		}

		if(VAL<=AR[Lbound])
		{
			return Lbound;
		}
		else
			if(Lbound<N-1)return Lbound+1;

	}
	inline void recomputeSum(double *probs, int numClass)
	{
		_sums = (double *) malloc(sizeof(double) * numClass);
		bzero((void *) _sums, numClass * sizeof(double));
		_sums[0] = probs[0];
		for(int i = 1; i < numClass; i++) {
		  _sums[i] = _sums[i-1] + probs[i];
		}
	}

	int generateRandomClass(double *probs, int numClass) {
	  if(_sums==NULL)
	  {
		recomputeSum(probs,numClass);
	  }

	  double random = rand()/((double)RAND_MAX + 1);

	  int index = bsearch(_sums,numClass,random);

	  return index;
	}

};

#endif // SYNTHETICGENERATOR_HPP
