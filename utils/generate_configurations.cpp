/*to compile
/*  -lgsl -lgslcblas
 */


#include <stdio.h>
#include <gsl/gsl_qrng.h>
//#include <gsl/gsl_rng.h>
#include <map>
#include <vector>
#include <iostream>
#include <time.h>
#include <set>

//we used struct just to help code reading (by looking at members name)
struct configuration_t{
	int id;			//the id represent the number of the configuration (used for ordering)
	int nw;
	int batch;
};

struct throughput_configuration_t{
	double expected_thr;
	double measured_thr;
};

struct classcomp {
  bool operator() (const configuration_t& a, const configuration_t& b) const
  {return a.id<b.id;}
};
int main (void)
{
	int i;
	gsl_qrng * q = gsl_qrng_alloc (gsl_qrng_sobol, 2);
	//if we want a random seed
//	gsl_rng_set( q, time(NULL));

	const std::vector<int> n_workers {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};
	const std::vector<int> batches { 384, 768, 1152, 1536, 1920, 2304, 2688, 3072, 3456, 3840, 4224, 4608, 4992, 5376, 5760};
	std::map<configuration_t,throughput_configuration_t,classcomp> map;
	std::set<std::pair<int,int>> generated_configuration; //used to skip duplicates


	//versione in cui generiamo lungo le due dimensioni e poi ne variamo una per volta (di fatto creiamo due configurazioni)

	//first of all insert the boundaries
	int num_conf=0;
	configuration_t  conf={num_conf++,n_workers.front(),batches.front()};
	map[conf]={0.0,0.0};
	generated_configuration.insert({conf.nw,conf.batch});
	conf={num_conf++,n_workers.front(),batches.back()};
	map[conf]={0.0,0.0};
	generated_configuration.insert({conf.nw,conf.batch});
	conf={num_conf++,n_workers.back(),batches.back()};
	map[conf]={0.0,0.0};
	generated_configuration.insert({conf.nw,conf.batch});
	conf={num_conf++,n_workers.back(),batches.front()};
	map[conf]={0.0,0.0};
	generated_configuration.insert({conf.nw,conf.batch});


	for (i = 0; i < 200; i++)
	{
		double v[2];
		int batch, nw;
		gsl_qrng_get (q, v); //da numeri tra zero e 1	\\todo: genera mai una delle ultime conf?
		//a questo punto vediamo, per ognuno dei due numeri, se diverso e in caso generiamo conf
		nw=n_workers[(int)(v[0]*n_workers.size())];
		batch=batches[(int)(v[1]*batches.size())];
//		printf("Generato: %d %d\n",nw,batch);
		if(nw!=conf.nw)
		{
			//check if it exists
			if(generated_configuration.find({nw,conf.batch})==generated_configuration.end()) //still not generated
			{
				conf.nw=nw;
				conf.id=num_conf++;
				map[conf]={0.0,0.0};
				generated_configuration.insert({conf.nw,conf.batch});
			}

		}
		if(batch!=conf.batch)
		{
			if(generated_configuration.find({conf.nw,batch})==generated_configuration.end()) //still not generated
			{
				conf.batch=batch;
				conf.id=num_conf++;
				map[conf]={0.0,0.0};
				generated_configuration.insert({conf.nw,conf.batch});
			}
		}
	}

	//check: only one dimension at a time

	configuration_t c={0,1,384};
	for(std::pair<configuration_t,throughput_configuration_t> a:map)
	{
		if(a.first.nw!=c.nw && a.first.batch !=c.batch)
			printf("!!!!!Errore su configurazione: ");
		printf("%d\t%d\t%d\n",a.first.id,a.first.nw,a.first.batch);
		c=a.first;

	}

	gsl_qrng_free (q);
	return 0;
}
