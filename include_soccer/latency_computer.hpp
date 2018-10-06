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




#if !defined(LATCOMPUTER_HPP)
#define LATCOMPUTER_HPP

#include <stdio.h>
#include <vector>
#include "general.hpp"

static int memstat(const char *pidstr, size_t &vmz, size_t &peak_vmz) {
	size_t len = 128;
	char _line[128];
	char *line = &_line[0], *vmsize = NULL, *vmpeak = NULL;
	FILE *f;

	f = fopen(pidstr, "r");
	if (!f) return 1;

	/* Read memory size data from /proc/pid/status */
	while (!vmsize || !vmpeak)    {
	if (getline(&line, &len, f) == -1) {
		/* Some of the information isn't there, die */
		return 1;
	}
	/* Find VmPeak */
	if (!strncmp(line, "VmPeak:", 7))  {
		vmpeak = strdup(&line[7]);
	}
	/* Find VmSize */
	else if (!strncmp(line, "VmSize:", 7)) {
		vmsize = strdup(&line[7]);
	}
	}
	fclose(f);

	/* Get rid of " kB\n"*/
	len = strlen(vmsize);
	vmsize[len - 4] = 0;
	len = strlen(vmpeak);
	vmpeak[len - 4] = 0;
	vmz = atol(vmsize);
	peak_vmz = atol(vmpeak);

	free(vmpeak);
	free(vmsize);
	//fprintf(stderr, "%ld\t%ld\n", vmz, peak_vmz);
	return 0;
}

class LatencyComputer {
public:
	LatencyComputer(long long initial_timestamp):
		initial_timestamp(initial_timestamp),print_rate((long long)(PRINT_RATE*1000)) {

		snprintf(pidstr, 512, "/proc/%d/status", getpid());
	}


	/**
	 * @brief init: to be called for initializing the metrics and printings
	 * @return
	 */
	bool init() {
		cumlat      = 0.0;
		latTotal = 0.0;
		results     = results_last_print = 0;
		results_tasks = 0;
		total_tasks = 0;
		last_print  = current_time_usecs();
		start_usecs = current_time_usecs();
		computation_times = 0;
		return true;
	}

	void terminate() {
		//print last latency
		double avglat=cumlat/results_last_print;
		size_t vmsize, peak_vmsize;
		printf("Time: %6.3f, recvd results: %d, avg latency (usec): %6.3f\n",((double)(current_time_usecs()-start_usecs))/1000000.0,results_last_print,avglat);
//		memstat(pidstr, vmsize, peak_vmsize);
//		printf("Time: %6.3f, recvd results: %d, avg latency (usec): %6.3f, VmSize: %ld, PeakVmSize: %ld\n",((double)(current_time_usecs()-start_usecs))/1000000.0,results_last_print,avglat, vmsize, peak_vmsize);
//		fprintf(fout,"Time: %6.3f, recvd results: %d, avg latency (usec): %6.3f, VmSize: %ld, PeakVmSize: %ld\n",((double)(current_time_usecs()-start_usecs))/1000000.0,results_last_print,avglat, vmsize, peak_vmsize);
		total_tasks += results_last_print;
		latencies.push_back(avglat);
		times.push_back((current_time_usecs()-start_usecs)/1000000.0);

		FILE *fout=fopen("latencies.dat", "w");
		for(int i=0;i<latencies.size();i++)
		{
			fprintf(fout, "%.1f\t%.1f\n",times[i],latencies[i]);
		}
		fclose(fout);

	}

	bool compute(const results_t &res) {
	//	if (res.type == -1) return false;
		//double lat=((double)((long long)(getticks()-res.timestamp)-(long long)start_ticks))/frequency;
		long lat=((current_time_usecs()-start_usecs)-(res.timestamp-initial_timestamp));

		cumlat+=lat;
		results_last_print++;
		results++;
		//computation_times+=res.computation_time;
		if(current_time_usecs()-last_print>print_rate) {
			//double time = ((double)(getticks()-start_ticks)/((long long)((unsigned long)(frequency)*1000000)));

			double curr_sec=((double)(current_time_usecs()-start_usecs))/1000000.0;
			double avglat=cumlat/results_last_print;
			results_tasks++;
			total_tasks += results_last_print;
			latTotal += cumlat;
#if !defined(SCRIPT_MODE)
			size_t vmsize, peak_vmsize;
//			memstat(pidstr, vmsize, peak_vmsize);
//			printf("Time: %6.3f, recvd results: %d, avg latency (usec): %6.3f, VmSize: %ld, PeakVmSize: %ld\n",curr_sec,results_last_print,avglat, vmsize, peak_vmsize);
			printf("Time: %6.3f, recvd results: %d, avg latency (usec): %6.3f\n",curr_sec,results_last_print,avglat);
//			fprintf(fout,"Time: %6.3f, recvd results: %d, avg latency (usec): %6.3f, VmSize: %ld, PeakVmSize: %ld\n",curr_sec,results_last_print,avglat, vmsize, peak_vmsize);
#endif
			//exit if latency over the threshold
			//if(avglat>MAX_LATENCY)
				//exit(-1);
			latencies.push_back(avglat);
			times.push_back((current_time_usecs()-start_usecs)/1000000.0);

			cumlat=0.0;
			results_last_print=0;
			last_print=current_time_usecs();
		}
		return true;
	}


	/**
	 * @brief getAvgTotalTasks get average number of produced results per second
	 * @return
	 */
	double getAvgTotalTasks() {
		return total_tasks/((current_time_usecs()-start_usecs)/1000000.0);
	}

	double getAvgLatency() {
		return latTotal/results;
	}

	int getNoSeconds() {
		return results_tasks;
	}

	double getComputationTime(){
		return (double)computation_times/results;
	}

	/**
	 * @brief getTime returns elapsed time from init
	 * @return
	 */
	double getTime()
	{
		return current_time_usecs()-start_usecs;
	}

private:
	const long long initial_timestamp=0;
	FILE *fout;
	double cumlat;
	int    results, results_last_print;
	int results_tasks;
	long total_tasks;
	double latTotal;
	ticks  last_print,start_ticks;
	const ticks print_rate;
	char pidstr[512];
	long start_usecs;
	long computation_times;
	std::vector<double> latencies;
	std::vector<double> times;

};

#endif /* LATCOMPUTER_HPP */
