/**

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
*/
 /* Test program for the Window Farming pattern over Multicore
 *
 * The computation performed regards the computation of a polynomial over the last
 * received quotes of a given simbol and the stats for computing candle sticks charts
 *
 * The structure is the following one
 *
 *                                                         +------------------------------------------------+
														   |                                                |
														   |          +---------> Worke+                    |
														   |          |                |                    |
														   |          |                |                    |
+------------------+           +----------------+          |          +---------> Work++                    |          +--------------+
|    Financial     +----------->   Data Recv    +---------->   Emitter|               +-+----->Collector    +---------->  Visualizer  |
|  Data Generator  |           |(Filtering Data)|          |          |                 |                   |          |              |
+------------------+           +----------------+          |          +---------> Worker+                   |          +--------------+
														   |                                                |
														   |                                                |
														   |                                                |
														   +Window Farming----------------------------------+

 *
 *	- We have a financial data generator that injects tupl
 *  - an internal data receiver that is in charge of receiving tuples, filtering (for the moment no filtering) and
 *			convert them in an internal data representation
 *  - the Window Farming that actually computes
 *  - a visualizer that receives data, (save it, optionally) and computes statistics
 */

/*
 * Author: Tiziano De Matteis <dematteis at di.unipi.it>
 */


#include <iostream>
#include <ff/node.hpp>
#include <ff/pipeline.hpp>
#include <ff/mapper.hpp>
#include "../include_financial/general.h"
#include "../include_financial/latency_computer.hpp"
#include "../include_financial/writer.hpp"
#include "../include/Basic_Window.hpp"
#include "../include/Win_WF.hpp"
#include "../include/Fake_Allocator.hpp"
#include "../include_financial/synthetic_generator.hpp"
using namespace ff;
using namespace std;

// function filterData applied by the first stage (it is empty for now)
inline void filterData(const tuple_t &in, tuple_t &res) {
	res = in;
}


// window parameters are global variables (read only after the initilization)
unsigned long window_size;
unsigned long window_slide;

// first stage of the pipeline (Generator)
template<typename ext_alloc_in_t=std::allocator<tuple_t>>
class DataReceiver: public ff_node_t<tuple_t> {
private:
	tuple_t taskFiltered;
	ext_alloc_in_t& alloc; // allocator of stream elements
	unsigned int num_keys;
	bool first;
	int *counters_id;

public:
	// constructor
	DataReceiver(ext_alloc_in_t& _alloc, unsigned int _keys): first(true), alloc(_alloc), num_keys(_keys)
	{
		counters_id=new int[num_keys]();
		std::fill_n(counters_id, num_keys, 0);
	}



	// svc method
	tuple_t* svc(tuple_t* t) {
		//printf("Received %Ld\n",t->id);
		tuple_t* taskFiltered = alloc.allocate(1);
		alloc.construct(taskFiltered);
		filterData(*t, *taskFiltered);
		taskFiltered->internal_id=counters_id[taskFiltered->type];
		counters_id[taskFiltered->type]++;
		taskFiltered->type++; // for uniformity the identifiers of the keys start from 1
		return taskFiltered;
	}
};

// last stage of the pipeline (Visualizer)
template<typename ext_alloc_out_t=std::allocator<winresult_t>>
class Visualizer: public ff_node_t<winresult_t> {
private:
	long counter;
	Writer &writer1;
	Writer &writer2;
	LatencyComputer lat;
	ext_alloc_out_t& alloc; // allocator of results
	unsigned long check_counter;

public:
	// constructor
	Visualizer(Writer &writer1, Writer &writer2, ext_alloc_out_t& _alloc)
		:counter(0), writer1(writer1), writer2(writer2), lat(0), alloc(_alloc),  check_counter(0) {
	}



	int svc_init()
	{
		lat.init();
		return 0;
	}

	// svc method
	winresult_t* svc(winresult_t* in) {

		winresult_t &result = *in;
		counter++;
		//Uncomment this if you want to save results
		//		writer1.writeCandle(result);
		//		writer2.writeFitting(result);
		lat.compute(result);
		// check the ordering of results
		if(check_counter!= in->id) {
			cout << "Results received out-of-order! ID: " << in->id << " KEY: " << in->type << endl;
		}
		check_counter++;
		// destroy and deallocate in
		alloc.destroy(in);
		alloc.deallocate(in, 1);
		return GO_ON;
	}

	// svc_end method
	void svc_end() {
		//cout << "Number of seconds of the execution: " << lat.getNoSeconds() << endl;
		printf("Average results processed per second: %.3f\n",lat.getAvgTotalTasks());
		printf("Average latency (usecs): %.3f\n", lat.getAvgLatency());
		printf("Total number of received results are: %ld\n", counter);
		printf("Average computation time (usec): %.3f\n",lat.getComputationTime());
		printf("Elapsed time (msec): %.3f\n",lat.getTime()/1000.0);
	}
};


//Function that must be executed by the program
void F(std::vector<tuple_t> &data, unsigned long size, winresult_t & res) {

	long start_usec=current_time_usecs();

	float sum=0;
	unsigned long starting_id = data[0].internal_id;
	long first_timestamp=data[0].original_timestamp;
	res.type=data[0].type;
	//Compute quadratic fitting

	//variables needed for the bid part
	float sum_x_bid=0, sum_y_bid=0, sum_x_square_bid=0, sum_x_cube_bid=0, sum_x_quad_bid=0,
			sum_x_times_y_bid=0, sum_x_sq_times_y_bid=0;
	float sum_x_ask=0, sum_y_ask=0, sum_x_square_ask=0, sum_x_cube_ask=0, sum_x_quad_ask=0,
			sum_x_times_y_ask=0, sum_x_sq_times_y_ask=0;

	int numb_points_bid=0, numb_points_ask=0;

	for(int j=0;j<NUM_IT;j++)
	{
		for(int i=0; i<size; i++) {
			float x=data[i].original_timestamp-first_timestamp;

			//bid part
			if(data[i].bid_size>0)
			{
					float y_bid=data[i].bid_price;
					sum_x_bid+=x;
					sum_y_bid+=y_bid;
					float xsq=x*x;
					sum_x_square_bid+=xsq;
					sum_x_cube_bid+=xsq*x;
					sum_x_quad_bid+=xsq*xsq;
					sum_x_times_y_bid+=x*y_bid;
					sum_x_sq_times_y_bid+=xsq*y_bid;
					numb_points_bid++;
			}


			//ask part
			if(data[i].ask_size>0)
			{
					float y_ask=data[i].ask_price;
					sum_x_ask+=x;
					sum_y_ask+=y_ask;
					float xsq=x*x;
					sum_x_square_ask+=xsq;
					sum_x_cube_ask+=xsq*x;
					sum_x_quad_ask+=xsq*xsq;
					sum_x_times_y_ask+=x*y_ask;
					sum_x_sq_times_y_ask+=xsq*y_ask;
					numb_points_ask++;
			}

			//skip consecutive elements with the same  x-value
			while(i<size-1 && data[i].original_timestamp==data[i+1].original_timestamp)
			{
				i++;
			}
		}
	}

	//compute bid coefficient
	float a_bid=((sum_x_sq_times_y_bid*sum_x_square_bid)-(sum_x_times_y_bid*sum_x_cube_bid));
	a_bid/=((sum_x_square_bid*sum_x_quad_bid)-sum_x_cube_bid*sum_x_cube_bid);

	float b_bid=((sum_x_times_y_bid*sum_x_quad_bid)-(sum_x_sq_times_y_bid*sum_x_cube_bid));
	b_bid/=((sum_x_square_bid*sum_x_quad_bid)-(sum_x_cube_bid*sum_x_cube_bid));

	float c_bid=sum_y_bid/numb_points_bid - b_bid*sum_x_bid/numb_points_bid - a_bid*sum_x_square_bid/numb_points_bid;

	res.p0_bid=c_bid;
	res.p1_bid=b_bid;
	res.p2_bid=a_bid;

	//compute ask coefficient

	float a_ask=((sum_x_sq_times_y_ask*sum_x_square_ask)-(sum_x_times_y_ask*sum_x_cube_ask));
	a_ask/=((sum_x_square_ask*sum_x_quad_ask)-sum_x_cube_ask*sum_x_cube_ask);

	float b_ask=((sum_x_times_y_ask*sum_x_quad_ask)-(sum_x_sq_times_y_ask*sum_x_cube_ask));
	b_ask/=((sum_x_square_ask*sum_x_quad_ask)-(sum_x_cube_ask*sum_x_cube_ask));

	float c_ask=sum_y_ask/numb_points_ask - b_ask*sum_x_ask/numb_points_ask - a_ask*sum_x_square_ask/numb_points_ask;

	res.p0_ask=c_ask;
	res.p1_ask=b_ask;
	res.p2_ask=a_ask;


	res.high_ask=sum;

	res.id = ((starting_id) / window_slide);
	res.timestamp=data[size-1].timestamp;

	// candle stick: for the candle stick graph we have to consider the last window_slide elements and compute the various values
	res.low_bid=10000;
	res.low_ask=10000;
	res.high_bid=0;
	res.high_ask=0;
	res.open_bid=0;
	res.open_ask=0;
	int last_valid_bid=0, last_valid_ask=0;
	for(int i=size-window_slide;i<size;i++) {
		// compute for both ask and bid (taking into account only vaules !=0)
		if(data[i].bid_size>0) {
			if(res.open_bid==0)
				res.open_bid=data[i].bid_price;
			if(data[i].bid_price>res.high_bid)
				res.high_bid=data[i].bid_price;
			if(data[i].bid_price<res.low_bid)
				res.low_bid=data[i].bid_price;
			last_valid_bid=i;
		}
		if(data[i].ask_size>0) {
			if(res.open_ask==0)
				res.open_ask=data[i].ask_price;
			if(data[i].ask_price>res.high_ask)
				res.high_ask=data[i].ask_price;
			if(data[i].ask_price<res.low_ask)
				res.low_ask=data[i].ask_price;
			last_valid_ask=i;
		}
	}
	res.close_bid=data[last_valid_bid].bid_price;
	res.close_ask=data[last_valid_ask].ask_price;
	res.computation_time=current_time_usecs()-start_usec;
}


int main(int argc, char *argv[]) {
	// input parameters
	if(argc < 6) {
		cout << "Usage: " << argv[0] << " <window_size> <window_slide> <nworkers> <rate> <num_tuples>" << endl;
		return -1;
	}
	// initialize global variables with the window size and slide
	window_size = atoi(argv[1]);
	window_slide = atoi(argv[2]);
	int num_keys = 1;
	int nworkers = atoi(argv[3]);
	int batch_size = 0;	//no batching for cpu
	double rate=atof(argv[4]);
	int num_tuples=atoi(argv[5]);

	cout << "Configuration: [Window_size " << window_size << "] [Window_slide " << window_slide << "][Num_worker " << nworkers << "] [Batching size " << batch_size << "]" <<  endl;
	// initialize Writer
	tuple_t taskIn;
	Writer writer1, writer2;
	if(!writer1.open("candle_sticks.dat")) {
		cout << "Cannot open DB1\n";
		return -1;
	}
	if(!writer2.open("fitting.dat")) {
		cout << "Cannot open DB2\n";
		return -1;
	}


	// function M: returns the id and the key of an input tuple
	auto M_IN = [](const tuple_t &t) {
		std::pair<int,long> r;
		r.first = t.type;
		r.second = t.internal_id;
		return r;
	};
	// function R: returns the id and the key of an output result
	auto M_OUT = [](const winresult_t &out) {
		std::pair<int,long> r;
		r.first = out.type;
		r.second = out.id;
		return r;
	};


	// creation of the Window Farming pattern
	bool ordering=true;
	int allocator_kind=0;
	// creation of external allocators (faked)
	Fake_Allocator<tuple_t> alloc_in;
	Fake_Allocator<winresult_t> alloc_out;
	// creation (with faked external allocators)
	// intialize LatencyComputer

	SyntheticGenerator sg(num_keys,rate,num_tuples);
	DataReceiver<Fake_Allocator<tuple_t>> generator(alloc_in, num_keys);
	Win_WF<tuple_t, winresult_t, Basic_Window<tuple_t> , Fake_Allocator<tuple_t>, Fake_Allocator<winresult_t>> wf(M_IN, M_OUT, F, nworkers, window_size, window_slide, alloc_in, alloc_out, false, batch_size, ordering,allocator_kind);
	Visualizer<Fake_Allocator<winresult_t>> visualizer(writer1, writer2,  alloc_out);
	// creation of the pipeline
	//initialize latency
	ff_Pipe<tuple_t, winresult_t> pipe(sg,generator, wf, visualizer);
	pipe.setXNodeInputQueueLength(2000000);
	pipe.setXNodeOutputQueueLength(2000000);
	pipe.run_and_wait_end();
#if defined(TRACE_FASTFLOW)
	pipe.ffStats(cout);	//used for performance debugging
#endif
	writer1.close();
	writer2.close();
	return 0;

}
