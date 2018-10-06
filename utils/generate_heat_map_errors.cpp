//Questo programma prende due file con tre colonne (no header)
//	NW	Batch	Rate
//e genera file per stampare heat map dell'errore tra file 1 e file 2
//nel file generato i dati sono per batch crescente (colonne) e numero
//worker crescente (righe)
//I due file di partenza sono ordinati


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

	//nota:npoints viene usato per stampare il titolo
	if(argc<4)
	{
		cerr<<"Usage: "<<argv[0]<<" <file1> <file2> <output>"<<endl;
		exit(-1);
	}
	int nw, batch;

	double thr;
	vector<configuration_t> thr1,thr2;

	ifstream file1(argv[1]);
	configuration_t conf;
	int nconf=0;
	while(file1 >> nw >> batch >> thr)
	{
		conf={nconf++,nw,batch,thr};
		thr1.push_back(conf);

	}
	file1.close();

	nconf=0;
	ifstream file2(argv[2]);
	while(file2 >> nw >> batch >> thr)
	{
		conf={nconf++,nw,batch,thr};
		thr2.push_back(conf);

	}
	file2.close();


	//ora generare heatmap
	ofstream fout(argv[3]);
	int i=0;
	double err=0;


#if defined(PRINT_LABELS)
	for(int b:batches)
		fout <<"\t"<< b;
	fout << endl;
#endif

	double min_err=1;
	double max_err=0;

	for(int n:n_workers)
	{

#if defined(PRINT_LABELS)
			fout << n;
#endif
		for(int b:batches)
		{
			//cout << thr1[i].thr<<" "<<thr2[i].thr<<": "<< abs((int)(thr1[i].thr-thr2[i].thr))/thr1[i].thr<<endl;
			double e=abs((int)(thr1[i].thr-thr2[i].thr))/thr1[i].thr;
			err+=e;
			fout<<"\t"<<e;
			if(e<min_err)
				min_err=e;
			if(e>max_err)
				max_err=e;
			i++;
		}
		fout <<endl;
	}
	fout << "# Average error: "<<err/thr1.size()<<" Min error: "<<min_err<<" Max error: "<<max_err<<endl;
	fout.close();
	cout << "# Average error: "<<err/thr1.size()<<" Min error: "<<min_err<<" Max error: "<<max_err<<endl;


}
