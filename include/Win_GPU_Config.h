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
  This file contains a set of configuration macro and useful information abount gpu, max number of workers
  and reconfigurations.

  For the moment being these are static (i.e. defined in this file and changes will require to recompile)
  and derived form the machine_configuration.h file.

*/

#ifndef WIN_GPU_CONFIG_H
#include <vector>
#include "../config/machine_configuration.h"
#define WIN_GPU_CONFIG_H


/**
    RECONFIGURATIONS DEFINITIONS
*/

//NOTE: if none of them is enable, no manager will be launched at all

#define HARD_RECONFIGURATIONS		//use Hard reconfigurations
//#define SOFT_RECONFIGURATIONS		//use Soft reconfigurations


//#define BRUTE_FORCE			//if defined, the Manager tries all the possible configuration in an exhaustive way. NOTE: one of the two previous must be defined. It may takes a lot of time
#if defined(BRUTE_FORCE)
	#define PARTIAL			//if used with the BRUTE_FORCE flag, allow to evaluate only the 20% of the overall configurations
#endif

const double _monitoring_interval=1000000.0;	//expressed in usec, used by the collector for monitoring the rate and communicate it to manager
const int _mov_average_length=3;	//the maximum number of the last monitor intervals that we monitor to check whether the throughput is stable
const int _cv_average_length=3;		//the maximum number of consecutive coeff variatians to consider to determine if the throughput is stable




#define INITIAL_CONFIG 5			//the first step at which start looking for the interpolation quality (typically set to 5)
#define ADDITIONAL_CONFIG 2			//how many additional configuration tests if we fail to find an optimal configuration
#define TOLERANCE_SEARCH 0.05		//tolerance with respect to target used when searching for optimal configuration
#define TOLERANCE_CHECK 0.04		//tolerance used when checking if the configuration is ok
#define TOLERANCE_EURISTIC2 0.02	//tolerance used with the second euristic (we are approaching limit condition)

#define	THR_STABLE_CV 0.05			//value of the coefficient of variation to consider the throughput stable

#define LOG_MANAGER					//the manager will print logging information into the manager.log file

/**
    GPU DEFINITIONS
*/
#define MAX_CUDA_THREADS_PER_BLOCK (2*CORE_PER_SM)	   //cudas threads for each block used in gpu versions. Recommended:CORE_PER_SM*2
#define MAX_BATCH_SIZE (MAX_CUDA_THREADS_PER_BLOCK*SM) //recommended: Number of SM in the GPU * MAX_CUDA_THREADS_PER_BLOCK


const int _starting_pardegree=1;
const int _starting_bs=MAX_CUDA_THREADS_PER_BLOCK;

/**
    CPU DEFINITIONS
*/

#define MAX_NUM_WORKERS REPLICA	//in the code workers is used instead of replica




/**
  GENERAL DEFINITIONS
  */

#define BOUNDED_QUEUE 1 //By default we use bounded queue Emitter-> Replicas to avoid having long transitory phase while reconfiguring

const bool ordering=true;	//ordering collector
const bool overlap_accumulation=false;	//do not overlap accumulation on worker with GPU computation

const int allocator_kind=0;		//type of allocator used inside the WF: 0= standard allocator, 2 = fastflow, 3 =static

/**
  MISCELLANEOUS
  */


#define REPEAT_25( x ) { x x x x x x x x x x x x x x x x x x x x x x x x x }
#define MIN(a,b) ((a) < (b) ? (a) : (b))

//Colored printf

#define RED_UNDERLINE "\x1b[31;4m"
#define RED     "\x1b[31m"
#define GREEN_BOLD   "\x1b[32;1m"
#define RED_BOLD "\x1b[31;1m"
#define YELLOW  "\x1b[33m"
#define BLUE    "\x1b[34m"
#define MAGENTA "\x1b[35m"
#define CYAN    "\x1b[36m"
#define CYAN_BOLD "\x1b[36;1m"
#define RESET   "\x1b[0m"





#endif // WIN_GPU_CONFIG_H
