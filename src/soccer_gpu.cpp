/**
 * GPU Version
 */
#include <iostream>
#include <ff/node.hpp>
#include <ff/pipeline.hpp>
#include <memory>
#include "../include_soccer/general.hpp"
#include "../include_soccer/generator.hpp"
#include "../include_soccer/latency_computer.hpp"
#include "../include/Win_GPU_WF.hpp"
#include "../include/GPU_CB_Window.hpp"


using namespace std;
using namespace ff;


//#define DUMP_RESULTS //use it to dump results on file

/**
 * @brief The Receiver class receives the data from the generator and create the internal tuple representation
 *	This add an additional field to the tuple (the centroid_id) that will be used by the computation
 *
 */

template<typename ext_alloc_in_t=std::allocator<point_t>>
class Receiver: public ff::ff_node_t<tuple_t,point_t>{

public:

	Receiver(ext_alloc_in_t &alloc) :_alloc_in(alloc)
	{}

	int svc_init()
	{
		_last_sent=current_time_usecs();
		return 0;
	}

	point_t* svc(tuple_t *tuple)
	{

		//create a copy for it (by filtering unecessary stuff)
		point_t* point=_alloc_in.allocate(1);
		_alloc_in.construct(point);
		//copy the data
		point->id=tuple->id;
		point->sid=tuple->sid;
		point->timestamp_usec=tuple->timestamp_usec;
		point->x=tuple->x;
		point->y=tuple->y;
		point->z=tuple->z;
		point->centroid_id=-1;

		_received++;
		if(_received%64==0 && current_time_usecs()-_last_sent>1000000)
		{
			//printf("Rate %.2f\n", (double)_received/((current_time_usecs()-_last_sent)/1000000.0));
			_last_sent=current_time_usecs();
			_received=0;
		}


		return point;
	}

private:
	ext_alloc_in_t &_alloc_in;
	int _received=0;
	long _last_sent=0;
};

template<typename ext_alloc_out_t=std::allocator<results_t>>
class Visualizer : public ff::ff_node_t<results_t>
{
public:

	Visualizer(ext_alloc_out_t &alloc):_alloc_out(alloc),_lat(0),_counter(0)
	{
#if defined(DUMP_RESULTS)
		_fout=fopen("clusters_gpu.dat","w");
#endif

	}

	int svc_init()
	{
		_lat.init();
		return 0;
	}

	results_t* svc(results_t* r)
	{

		//TODO controllo ordinamento
		//		printf("[%d]\n",r->id);

		if(r->id != _counter )
			std:: cout << "Results received out-of-order! ID: " << r->id << std::endl;

		_lat.compute(*r);
#if defined (DUMP_RESULTS)
		fprintf(_fout, "%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\n",r->timestamp,r->centroids[0].x, r->centroids[0].y,
				r->centroids[1].x, r->centroids[1].y,r->centroids[2].x, r->centroids[2].y);
#endif
		//destroy and deallocate
		_alloc_out.destroy(r);
		_alloc_out.deallocate(r,1);
		_counter++;
		return GO_ON;

	}

	void svc_end()
	{
		_lat.terminate();
		//cout << "Number of seconds of the execution: " << lat.getNoSeconds() << endl;
		printf("Average results processed per second: %.3f\n",_lat.getAvgTotalTasks());
		printf("Average latency (usecs): %.3f\n", _lat.getAvgLatency());
		printf("Total number of received results are: %ld\n", _counter);;
		printf("Elapsed time (msec): %.3f\n",_lat.getTime()/1000.0);
#if defined (DUMP_RESULTS)
		fclose(_fout);
#endif
	}

private:
	ext_alloc_out_t &_alloc_out;
	LatencyComputer _lat;
	int _counter;
	FILE *_fout;
};

/**********************************************************************************
 Kmeans part
*/

//NOTE: come struttura dati usiamo direttamente la tupla (sfruttiamo x e y)
const int numb_clusters=3;
const int max_iter=10;
__device__ double calculateDistance(point_t &a, point_t &b)
{
	double dist = 0.0;
	double tmp;
	tmp=a.x-b.x;
	dist+=tmp*tmp;
	tmp=a.y-b.y;
	dist+=tmp*tmp;
	return sqrt(dist);
}


//temp
void printPoint(point_t p)
{
	//std::cout<<"("<<p.x<<", "<<p.y<<")"<<std::endl;
	std::cout<<p.x<<" "<<p.y<<std::endl;
}


void printCluster(point_t centroids[],int k)
{
	for(int i=0;i<k;i++)
	{
		std::cout <<"Centroid "<<i<<": "; printPoint(centroids[i]);
	}
}

__device__ void AssignAndcomputeNewCluster(point_t *data, point_t *centroids, char *point2centr,int size, int k)
{

	int count[3];
	double tmpx[3];
	double tmpy[3];

	for(int i=0;i<k;i++)
	{
		count[i]=0;
		tmpx[i]=0;
		tmpy[i]=0;
	}
	for(int j=0;j<size;j++)
	{
		//Assign the point to a cluster
		int assigned_cluster = 0;
		double min_dist = calculateDistance(data[j],centroids[0]);

		//	std::cout << "Distance from 0 "<< min_dist<<std::endl;
		for (int i = 1; i<k; i++) {
			double dist = calculateDistance(data[j], centroids[i]);


			if (dist < min_dist) {
				min_dist = dist;
				assigned_cluster = i;
			}
			//	std::cout << "Distance from " <<i <<" "<< dist<<std::endl;
		}

		//add to the proper centroid
		count[assigned_cluster]++;
		tmpx[assigned_cluster]+=data[j].x;
		tmpy[assigned_cluster]+=data[j].y;
	}

	for(int i=0;i<k;i++)
	{
		centroids[i].x=tmpx[i]/count[i];
		centroids[i].y=tmpy[i]/count[i];
		//		printf("Centroid %d (%.2f, %.2f) has %d points\n",i,centroids[i].x, centroids[i].y,count[i]);
	}
}

int main(int argc, char *argv[])
{

	if(argc < 6)
	{
		fprintf(stderr, "Usage %s: <dataset> <window_size> <window_slide> <nworkers> <batch> <ntuples> [speedup_factor]\n",argv[0]);
		exit(-1);
	}

	int window_size=atoi(argv[2]);
	int window_slide=atoi(argv[3]);
	int nworkers=atoi(argv[4]);
	int batch_size=atoi(argv[5]);
	int ntuples=atoi(argv[6]);
	int speedup=1;
	if(argc>7)
		speedup=atoi(argv[7]);

	// function M: returns the id and the key of an input tuple
	//tuple must start with id=1. since we are receiving with id
	auto M_IN = [](const point_t &t) {
		std::pair<int,long> r;
		r.first = 0;		//no key here
		r.second = t.id;	//ids start from zero
		return r;
	};

	// function M_OUT: returns the id and the key of an output result
	auto M_OUT = [](const results_t &out) {
		std::pair<int,long> r;
		r.first = 1;		//no key here
		r.second = out.id;
		return r;
	};

	//size_t limit;
	//	cudaDeviceSetLimit(cudaLimitMallocHeapSize,1024000000);
	//cudaDeviceGetLimit(&limit,cudaLimitMallocHeapSize);
	//printf("Limit: %d\n",limit);

	auto lambda4 = [=] __device__(point_t* data, results_t *res,unsigned long size, void *scratch)
	{

		//compute the kmeans
		char *point2centr=(char *)scratch;
		//aggiunto per evitare chiamata

		for(int i=0;i<numb_clusters;i++)
		{
			res->centroids[i]=data[i];

		}

		//in this case we have a fixed number of iteration, no convergence
		//we can blindly assign point to cluster without controlling if anything has changed
		int iteration=0;

		//TODO qui fissato per evitare oscillazioni rate
		while(/*changed &&*/ iteration < max_iter)
		{
			AssignAndcomputeNewCluster(data, res->centroids,point2centr,size,numb_clusters);


			//	printCluster(res.centroids,numb_clusters);
			iteration++;
		}


		int starting_id = data[0].id;
		res->id = ((starting_id ) / window_slide);

		//for computing the latency we save the timestamp of the last tuple before the window triggering
		res->timestamp=data[size-1].timestamp_usec;

	};

	allocator<point_t> allocator_in;
	allocator<results_t> allocator_out;
	Generator gen(argv[1],ntuples,speedup);
	Receiver<> recv(allocator_in);
	Visualizer<> vis(allocator_out);
	int exp_num_results_second=((int)11800*speedup)/window_slide;

	Win_GPU_WF<point_t, results_t, GPU_CB_Window<point_t>, decltype(lambda4)> wf(M_IN, M_OUT, lambda4, nworkers, window_size,
																				 window_slide, allocator_in, allocator_out, batch_size,exp_num_results_second,window_size);

	ff::ff_Pipe<tuple_t> pipe(gen,recv,wf,vis);
	//three steps needed to be able to add/remove workers
	pipe.run_then_freeze();
	pipe.wait_freezing();
	pipe.wait();
#if defined (TRACE_FASTFLOW)
	pipe.ffStats(cout);
#endif


}
