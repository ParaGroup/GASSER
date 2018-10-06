/**
 * Test program for the Window Farming pattern over GPU
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
														   |                         |                      |
														   |                         >-------- GPU          |
														   |                                                |
														   +Window Farming----------------------------------+

 *
 *	- We have a financial data generator that injects tuples (it can be an external process or a fastflow node
 *  - an internal data receiver that is in charge of receiving tuples, filtering (for the moment no filtering) and
 *			convert them in an internal data representation
 *  - the Window Farming that actually computes: each workers accumulates a certain number of consecutive windows
 *					that are then sent to the GPU. Each Cuda core will perform the computation on a different window
 *  - a visualizer that receives data, (save it) and computes statistics
 *
 * NOTE: with respects to the classical WF over CPU here we have changes in the scheduling policy (B=batch consecutive
 *		windows are sent to the same worker). Consequentely we have a different management of data in window
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
#include "../include/GPU_CB_Window.hpp"
#include "../include/Win_GPU_WF.hpp"
#include "../include/Fake_Allocator.hpp"
#include "../include_financial/synthetic_generator.hpp"


using namespace ff;
using namespace std;

// function filterData applied by the first stage (it is empty for now)
inline void filterData(const tuple_t &in, tuple_t &res) {
	res = in;
}


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
	DataReceiver( ext_alloc_in_t& _alloc, unsigned int _keys): first(true), alloc(_alloc), num_keys(_keys)
	{
		counters_id=new int[num_keys]();
		std::fill_n(counters_id, num_keys, 0);
	}

	// svc method
	tuple_t* svc(tuple_t* t) {

		tuple_t* taskFiltered = alloc.allocate(1);
		alloc.construct(taskFiltered);
		filterData(*t, *taskFiltered);
		taskFiltered->internal_id=counters_id[taskFiltered->type];
		counters_id[taskFiltered->type]++;
		taskFiltered->type++; // for uniformity the identifiers of the keys start from 1
#if !defined(PREGENERATE)
		//tuples are generated on the fly: we can erase them to save space
		delete t;
#endif
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
	long check_counter;

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
		//if enabled these save results but it costs a litle bit
	//	writer1.writeCandle(result);
	//	writer2.writeFitting(result);
		lat.compute(result);
		// check the ordering of results
		if(check_counter != in->id) {
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
		lat.terminate();
		//cout << "Number of seconds of the execution: " << lat.getNoSeconds() << endl;
		printf("Average results processed per second: %.3f\n",lat.getAvgTotalTasks());
		printf("Average latency (usecs): %.3f\n", lat.getAvgLatency());
		printf("Total number of received results are: %ld\n", counter);;
		printf("Elapsed time (msec): %.3f\n",lat.getTime()/1000.0);
	}
};



// main
int main(int argc, char *argv[]) {
	// input parameters
	if(argc < 7) {
		cout << "Usage: " << argv[0] << " <window_size> <window_slide> <nworkers> <batch> <rate> <num_tuples>" << endl;
		return -1;
	}
	// initialize global variables with the window size and slide
	int window_size = atoi(argv[1]);
	int window_slide = atoi(argv[2]);
	int num_keys = 1;
	int nworkers = atoi(argv[3]);
	int batch_size = atoi(argv[4]);
	double rate=atof(argv[5]);
	int num_tuples=atoi(argv[6]);



	cout << "Configuration: [Window_size " << window_size << "] [Window_slide " << window_slide << "][Num_worker " << nworkers << "] [Batching size " << batch_size << "]" << "[Num It = "<<NUM_IT<<"]"<< endl;
	// initialize Writer
	FILE *db1, *db2;
	tuple_t taskIn;
	Writer writer1, writer2;
	if(!writer1.open("candle_sticks_gpu.dat")) {
		cout << "Cannot open DB1\n";
		return -1;
	}
	if(!writer2.open("fitting_gpu.dat")) {
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

	//BUSINESS LOGIC
	auto lambda = [=] __device__ (tuple_t *data, winresult_t * res, int size, char * ) {

		float sum=0;
		unsigned long starting_id = data[0].internal_id;
		long first_timestamp=data[0].original_timestamp;
		res->type=data[0].type;
		/* Compute a, b and c such that:
					y= ax^2 +bx +c
		 * represent the best fitting of that points
		 *
		 * Source: https://www.easycalculation.com/statistics/learn-quadratic-regression.php
		 *
		 */

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
					i++;


			}
		}

		//compute bid coefficient
		float a_bid=((sum_x_sq_times_y_bid*sum_x_square_bid)-(sum_x_times_y_bid*sum_x_cube_bid));
		a_bid/=((sum_x_square_bid*sum_x_quad_bid)-sum_x_cube_bid*sum_x_cube_bid);

		float b_bid=((sum_x_times_y_bid*sum_x_quad_bid)-(sum_x_sq_times_y_bid*sum_x_cube_bid));
		b_bid/=((sum_x_square_bid*sum_x_quad_bid)-(sum_x_cube_bid*sum_x_cube_bid));

		float c_bid=sum_y_bid/numb_points_bid - b_bid*sum_x_bid/numb_points_bid - a_bid*sum_x_square_bid/numb_points_bid;

		res->p0_bid=c_bid;
		res->p1_bid=b_bid;
		res->p2_bid=a_bid;


		//compute ask coefficient

		float a_ask=((sum_x_sq_times_y_ask*sum_x_square_ask)-(sum_x_times_y_ask*sum_x_cube_ask));
		a_ask/=((sum_x_square_ask*sum_x_quad_ask)-sum_x_cube_ask*sum_x_cube_ask);

		float b_ask=((sum_x_times_y_ask*sum_x_quad_ask)-(sum_x_sq_times_y_ask*sum_x_cube_ask));
		b_ask/=((sum_x_square_ask*sum_x_quad_ask)-(sum_x_cube_ask*sum_x_cube_ask));

		float c_ask=sum_y_ask/numb_points_ask - b_ask*sum_x_ask/numb_points_ask - a_ask*sum_x_square_ask/numb_points_ask;

		res->p0_ask=c_ask;
		res->p1_ask=b_ask;
		res->p2_ask=a_ask;


	//	printf("%f %f %f %f %d %Ld \n",a_bid,b_bid,c_bid,sum_y_ask/numb_points_ask,numb_points_ask, data[size-1].original_timestamp-first_timestamp);

		res->high_ask=sum;
		// determine the id of the result
		res->id = ((starting_id ) / window_slide);
		res->timestamp=data[size-1].timestamp;
	//	printf("Compute!! on type %Ld - Starting: %Ld ID :%Ld \n",data[0].type,starting_id,res.id);



		// candle stick: for the candle stick graph we have to consider the last window_slide elements and compute the various values

	   res->low_bid=10000;
	   res->low_ask=10000;
	   res->high_bid=0;
	   res->high_ask=0;
	   res->open_bid=0;
	   res->open_ask=0;
	   int last_valid_bid=0, last_valid_ask=0;
	   for(int i=size-window_slide;i<size;i++) {
		   // compute for both ask and bid (taking into account only vaules !=0)
		   if(data[i].bid_size>0) {
			   if(res->open_bid==0)
				   res->open_bid=data[i].bid_price;
			   if(data[i].bid_price>res->high_bid)
				   res->high_bid=data[i].bid_price;
			   if(data[i].bid_price<res->low_bid)
				   res->low_bid=data[i].bid_price;
			   last_valid_bid=i;
		   }
		   if(data[i].ask_size>0) {
			   if(res->open_ask==0)
				   res->open_ask=data[i].ask_price;
			   if(data[i].ask_price>res->high_ask)
				   res->high_ask=data[i].ask_price;
			   if(data[i].ask_price<res->low_ask)
				   res->low_ask=data[i].ask_price;
			   last_valid_ask=i;
		   }
	   }
	   res->close_bid=data[last_valid_bid].bid_price;
	   res->close_ask=data[last_valid_ask].ask_price;
	};



	// creation of the Window Farming pattern
	  bool ordering=true;
	  bool overlap=false;
	  // creation of external allocators (faked)
	  Fake_Allocator<tuple_t> alloc_in;
	  Fake_Allocator<winresult_t> alloc_out;

	  // creation (with faked external allocators)
	  // intialize LatencyComputer
	  if (overlap)
		  printf("[Overlapping Enabled]\n");

	  SyntheticGenerator sg(num_keys,rate,num_tuples);
	  int exp_num_results_second=((int)rate)/window_slide;
	  DataReceiver<Fake_Allocator<tuple_t>> generator(alloc_in, num_keys);
	  Win_GPU_WF<tuple_t, winresult_t, GPU_CB_Window<tuple_t>, decltype(lambda), Fake_Allocator<tuple_t>, Fake_Allocator<winresult_t>> wf(M_IN, M_OUT, lambda, nworkers, window_size, window_slide, alloc_in, alloc_out,
																																		   batch_size,exp_num_results_second);
	  Visualizer<Fake_Allocator<winresult_t>> visualizer(writer1, writer2, alloc_out);
	  // creation of the pipeline
	  ff_Pipe<tuple_t, winresult_t> pipe(sg,generator, wf, visualizer);
	  pipe.setXNodeInputQueueLength(20000);
	  pipe.setXNodeOutputQueueLength(20000);

	  //three steps are Mandatory (TODO include in the support)
	  pipe.run_then_freeze();
	  pipe.wait_freezing();
	  pipe.wait();
#if defined (TRACE_FASTFLOW)
	  pipe.ffStats(cout);
#endif
	  writer1.close();
	  writer2.close();
	  return 0;

}





