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
 * Author: Tiziano De Matteis <dematteis at di.unipi.it>
 */



/**
  Data Generator for the soccer use case
*/


#ifndef GENERATOR_HPP
#define GENERATOR_HPP

#include <iostream>
#include <ff/node.hpp>
#include <ff/pipeline.hpp>
#include "../include_soccer/general.hpp"


using namespace std;

class Generator: public ff::ff_node_t<tuple_t>{
public:
	/**
	 * @brief Generator
	 * @param filename	the file from which read the tuples
	 * @param ntuples number of tupples
	 * @param speed_factor how many time we should accellerate
	 */
	Generator(char *filename, int ntuples, int speed_factor):_ntuples(ntuples), _speed_factor(speed_factor)
	{
		FILE *fin=fopen(filename,"rb");
		if(!fin)
		{
			fprintf(stderr,"Error in opening file\n");
			abort();
		}

		int actual_ntuples=min(_ntuples,_max_tuples); //if we indicate more tuples than available
		if(ntuples>_max_tuples)
			printf("[Generator] Required more tuples than present in the dataset. They will be replicated\n");
		printf("Reading %d tuples....\n",actual_ntuples);

		posix_memalign((void **)&_tuples,CACHE_LINE_SIZE,actual_ntuples*sizeof(tuple_t));
		fread(_tuples,sizeof(tuple_t),min(_ntuples,_max_tuples),fin);

		fclose(fin);

		//adjust the timestamp according to the speed_factor
		if(speed_factor>1)
		{
			for(int i=0;i<actual_ntuples;i++)
				_tuples[i].timestamp_usec/=speed_factor;
			printf("Original dataset is accellerated by a factor of %d\n",speed_factor);
		}

		//compute the theoretic avg rate
		double time_sec= ((_tuples[actual_ntuples-1].timestamp_usec-_tuples[0].timestamp_usec)/1000000.0);

		printf("[Generator] Theoretical rate: %.3f tuples/sec\n",actual_ntuples/time_sec);




	}

	~Generator()
	{
		free(_tuples);
	}

	void override_rate()
	{
		//TODO se necessario
	}

	tuple_t* svc(tuple_t*) {


		//TODO qui va messo un campo per calcolare la latenza o capire come fare
		long start_t=current_time_usecs();
		long last_recording=current_time_usecs();
		int overall_tuples_sent=0;
		int tuples_sent=0;
		long start_time_nsecs=current_time_nsecs();
		long start_timestamp_nsecs=(long)_tuples[0].timestamp_usec*1000;
		long next_send_time_nsecs=0;
		for(int i=0;i<min(_ntuples,_max_tuples);i++)
		{
			next_send_time_nsecs=(long)_tuples[i].timestamp_usec*1000 -start_timestamp_nsecs;
			volatile long end_wait=start_time_nsecs+next_send_time_nsecs;
			volatile long curr_t=current_time_nsecs();
			while(curr_t<end_wait)
				curr_t=current_time_nsecs();
			while(!ff_send_out(&_tuples[i]));
			tuples_sent++;
			overall_tuples_sent++;
			if(current_time_usecs()-last_recording>1000000)
			{
				_tuples_per_sec.push_back(tuples_sent);
				tuples_sent=0;
				last_recording=current_time_usecs();
			}

		}

		//printf("Last tuple sent id: %d, next tuple sent id: %d\n",_tuples[_max_tuples-1].id, overall_tuples_sent);

		if(_ntuples>_max_tuples)
		{
			//a certain number of tuples must be replicated
			//we have to adjust the timestamp and id
			int remaining_tuples=max(_ntuples-_max_tuples,0);
			long replica_start_time=0;
			long prev_replica_start_time=0;
			while(remaining_tuples>0)
			{
				//NOTE: does not work so well, I don't know why
				replica_start_time=current_time_nsecs()-start_time_nsecs;
				printf("[Generator] Replicating %d tuples. Replica start_time %.3f, \n",remaining_tuples,replica_start_time/1000000.0);
				for(int i=0;i<min(remaining_tuples,_max_tuples);i++)
				{
					//adjust the timestamp and id of the tuple
//					printf("Tuple timestamp: %ld\n",_tuples[i].timestamp_usec);
//					printf("Replica starting time: %ld\n",replica_start_time);
					_tuples[i].timestamp_usec+=(replica_start_time-prev_replica_start_time)/1000.0;
					_tuples[i].id=overall_tuples_sent;
					next_send_time_nsecs=(long)_tuples[i].timestamp_usec*1000;
					volatile long end_wait=start_time_nsecs+next_send_time_nsecs;
//					printf("Current time: %ld\n next_send_wait: %ld\n end wait: %ld\n",current_time_nsecs(),next_send_time_nsecs,end_wait);
					volatile long curr_t=current_time_nsecs();
					while(curr_t<end_wait)
						curr_t=current_time_nsecs();
					while(!ff_send_out(&_tuples[i]));

					tuples_sent++;
					overall_tuples_sent++;
					if(current_time_usecs()-last_recording>1000000)
					{
						_tuples_per_sec.push_back(tuples_sent);
						tuples_sent=0;
						last_recording=current_time_usecs();
					}
				}
				remaining_tuples=max(remaining_tuples-_max_tuples,0);
				prev_replica_start_time=replica_start_time;

			}

		}
		printf("End generator. Rate generator: %f \n",_ntuples/((current_time_usecs()-start_t)/1000000.0));
		FILE *fout=fopen("generator.dat","w");
		for(int r:_tuples_per_sec)
			fprintf(fout,"%d\n",r);
		fclose(fout);

		return EOS;


	}




private:
	tuple_t *_tuples;				//the data to send
	int _ntuples;
	int _speed_factor;
	std::vector<int> _tuples_per_sec;
	const int _max_tuples=39229326;		//in the dataset we have 41M of rows. If required we have to reply them

};

#endif // GENERATOR_HPP
