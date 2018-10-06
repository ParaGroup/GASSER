/***
 * This utility takes in input a file organized into three column
 *		nw	batch	rate
 *
 * it could be rate meausred or interpolated BUT IN ORDER
 * Then it generates an output file containing a matrix
 * (nw*batch) where each value is the rate of the corresponding matrix
 */

#include <iostream>
#include <fstream>
#include <vector>
using namespace std;

#define PRINT_LABELS	//print also the column and row labels

//we used struct just to help code reading (by looking at members name)
struct configuration_t{
	int id;			//the id represent the number of the configuration (used for ordering)
	int nw;			//the number of workers
	int batch;		//the batch size
	double thr;
};
//Comparator used for map ordering
struct classcomp {
  bool operator() (const configuration_t& a, const configuration_t& b) const
  {return a.id<b.id;}
};

const std::vector<int> n_workers {1,2,3,4,5,6,7,8,9,10};
const std::vector<int> batches { 384, 768, 1152, 1536, 1920, 2304, 2688, 3072, 3456, 3840, 4224, 4608, 4992, 5376, 5760};

/*std::map<configuration_t,double,classcomp> configurations1;
//insert in map
conf={nconf++,nw,batch};
configurations1[conf]=thr;*/
int main(int argc, char *argv[])
{

	if(argc<4)
	{
		cerr<<"Usage: "<<argv[0]<<" <file> <slide> <output file> "<<endl;
		exit(-1);
	}
	int nw, batch;
	double thr;
	vector<configuration_t> thr1;
	double slide=atof(argv[2]);
	ifstream file1(argv[1]);
	configuration_t conf;
	int nconf=0;
	while(file1 >> nw >> batch >> thr)
	{
		conf={nconf++,nw,batch,thr};
		thr1.push_back(conf);

	}
	file1.close();

	//ora generare heatmap
	ofstream fout(argv[3]);
	int i=0;

	fout << "#Incoming sustainable rate (i.e. throghput * slide)"<<endl;

#if defined(PRINT_LABELS)
	for(int b:batches)
		fout <<"\t"<< b;
	fout << endl;
#endif

	for(int n:n_workers)
	{
#if defined(PRINT_LABELS)
			fout << n;
#endif
		for(int b:batches)
		{
			fout<<"\t"<<thr1[i].thr*slide;
			i++;
		}
		fout <<endl;
	}
	fout.close();


}
