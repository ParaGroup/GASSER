/**
 *	CPU VERSION
 */

#include <iostream>
#include <ff/node.hpp>
#include <ff/pipeline.hpp>
#include <memory>
#include "../include_soccer/general.hpp"
#include "../include_soccer/generator.hpp"
#include "../include/Win_WF.hpp"
#include "../include/Basic_Window.hpp"
#include "../include_soccer/latency_computer.hpp"

using namespace std;

//these are declared global since the kernel must known it (required to compute the id of the result)
unsigned int window_size;
unsigned int window_slide;

//#define DUMP_RESULTS

/**
 * @brief The Receiver class receives the data from the generator and create the internal tuple representation
 *	This add an additional field to the tuple (the centroid_id) that will be used by the computation
 *
 */

template<typename ext_alloc_in_t=std::allocator<point_t>>
class Receiver: public ff::ff_node_t<tuple_t,point_t>{


public:

	Receiver(ext_alloc_in_t &alloc) :_alloc_in(alloc)
	{

	}

	point_t* svc(tuple_t *tuple)
	{
		//create a copy for it

		//TODO capire se poi serve o meno
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
		return point;
	}

private:
	ext_alloc_in_t &_alloc_in;
	int _received=0;



};

template<typename ext_alloc_out_t=std::allocator<results_t>>
class Visualizer : public ff::ff_node_t<results_t>	//TODO: definire tipo risultato
{
public:

	Visualizer(ext_alloc_out_t &alloc):_alloc_out(alloc),_lat(0),_counter(0)
	{
#if defined(DUMP_RESULTS)
		_fout=fopen("clusters.dat","w");
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
// Kmeans part
*/
//NOTE: come struttura dati usiamo direttamente la tupla (sfruttiamo x e y)
const int numb_clusters=3;
const int max_iter=10;
double calculateDistance(point_t &a, point_t &b)
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
bool assignToCluster(std::vector<point_t> &points, point_t centroids[], int k) {

	bool changed = false;
	// Assign each point to the nearest cluster: iterate over all points, find
	// the nearest cluster, and assign.
	for (int j=0;j<points.size();j++) {

		int new_cluster = 0;
		double min_dist = calculateDistance(points[j],centroids[0]);

		//	std::cout << "Distance from 0 "<< min_dist<<std::endl;
		for (int i = 1; i<k; i++) {
			double dist = calculateDistance(points[j], centroids[i]);
			if (dist < min_dist) {
				min_dist = dist;
				new_cluster = i;
			}
			//	std::cout << "Distance from " <<i <<" "<< dist<<std::endl;
		}

		changed = changed || (points[j].centroid_id!=new_cluster);
		points[j].centroid_id=new_cluster;
		//std::cout << "Assigned point "; printPoint(p); std::cout<< " to cluster: "
		// << new_cluster<< " " <<p.centroid << std::endl;
	}



	return changed;
}

void computeNewCluster(std::vector<point_t> &data, point_t centroids[], int k)
{
	//for each cluster (numbered from 0 to K) scan all the points and compute the average
	//since we are in 2d
	double xsum, ysum;
	int count;

	for(int i=0;i<k;i++)
	{
		xsum=0;ysum=0;
		count=0;

		for(int j=0;j<data.size();j++)
		{
			if(data[j].centroid_id==i)
			{
				count++;
				xsum+=data[j].x;
				ysum+=data[j].y;
			}
		}
		//	std::cout << "Centroid "<<i<<" had "<< count << " points"<<std::endl;
		//compute new centroid as the mean
		centroids[i].x=xsum/count;
		centroids[i].y=ysum/count;
	}


}


void Kmeans(std::vector<point_t> &data, unsigned long size, results_t & res)
{

	//compute the kmeans
	//id must be produced starting from 1
	res.id = ((data[0].id ) / window_slide);


	for(int i=0;i<numb_clusters;i++)
	{
		res.centroids[i]=data[i];

	}

	//assign points to centroids
	bool changed=assignToCluster(data,res.centroids,numb_clusters);
	int iteration=0;

	while(/*changed &&*/ iteration < max_iter)
	{
		computeNewCluster(data, res.centroids,numb_clusters);
		changed=assignToCluster(data,res.centroids,numb_clusters);
		//	printCluster(res.centroids,numb_clusters);

		iteration++;
	}

	int starting_id = data[0].id;

	res.id = ((starting_id) / window_slide);

	//for computing the latency we save the timestamp of the last tuple before the window triggering
	res.timestamp=data[size-1].timestamp_usec;
}

void AssignAndComputeNewCluster(std::vector<point_t> &data, point_t centroids[], int k)
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

	for(int i=0;i<data.size();i++)
	{
		//Assign the point to a cluster
		int assigned_cluster = 0;
		double min_dist = calculateDistance(data[i],centroids[0]);

		//	std::cout << "Distance from 0 "<< min_dist<<std::endl;
		for (int j = 1; j<k; j++) {
			double dist = calculateDistance(data[i], centroids[j]);


			if (dist < min_dist) {
				min_dist = dist;
				assigned_cluster = j;
			}
			//	std::cout << "Distance from " <<i <<" "<< dist<<std::endl;
		}

		//add to the proper centroid
		count[assigned_cluster]++;
		tmpx[assigned_cluster]+=data[i].x;
		tmpy[assigned_cluster]+=data[i].y;
	}

	for(int i=0;i<k;i++)
	{
		centroids[i].x=tmpx[i]/count[i];
		centroids[i].y=tmpy[i]/count[i];
	}

}

void Kmeans2(std::vector<point_t> &data, unsigned long size, results_t & res)
{
	res.id = ((data[0].id ) / window_slide);


	for(int i=0;i<numb_clusters;i++)
	{
		res.centroids[i]=data[i];

	}

	//assign points to centroids
	int iteration=0;

	while(iteration < max_iter)
	{

		AssignAndComputeNewCluster(data,res.centroids,numb_clusters);

		iteration++;
	}

	int starting_id = data[0].id;

	res.id = ((starting_id) / window_slide);

	//for computing the latency we save the timestamp of the last tuple before the window triggering
	res.timestamp=data[size-1].timestamp_usec;
}

int main(int argc, char *argv[])
{

	printf("Sizeof: %d\n",sizeof(tuple_t));
	if(argc < 5)
	{
		fprintf(stderr, "Usage %s: <dataset> <window_size> <window_slide> <nworkers> <ntuples> [<speedup_factor>]\n",argv[0]);
		exit(-1);
	}

	window_size=atoi(argv[2]);
	window_slide=atoi(argv[3]);
	int nworkers=atoi(argv[4]);
	int ntuples=atoi(argv[5]);
	int speedup=1;
	if(argc>6)
		speedup=atoi(argv[6]);


	// function M: returns the id and the key of an input tuple
	//tuple must start with id=1. since we are receiving with id
	auto M_IN = [](const point_t &t) {
		std::pair<int,long> r;
		r.first = 0;		//no key here
		r.second = t.id;	//ids start from zero
		return r;
	};

	// function R: returns the id and the key of an output result
	auto M_OUT = [](const results_t &out) {
		std::pair<int,long> r;
		r.first = 1;		//no key here
		r.second = out.id;
		return r;
	};

	//appiccica qui wf e vedi se va


	allocator<point_t> allocator_in;
	allocator<results_t> allocator_out;
	Generator gen(argv[1],ntuples,speedup);
	Receiver<> recv(allocator_in);
	Visualizer<> vis(allocator_out);
	//In this case it is convenient to avoid batching here
	Win_WF<point_t, results_t, Basic_Window<point_t>> wf(M_IN,M_OUT,Kmeans2,nworkers,window_size,window_slide,allocator_in,allocator_out,false,0,true);

	ff::ff_Pipe<tuple_t> pipe(gen,recv,wf,vis);
	pipe.run_and_wait_end();



}
