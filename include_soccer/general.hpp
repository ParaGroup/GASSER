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
  This file contains general definition (e.g. data types)
  for the soccer use case
*/

#ifndef GENERAL_HPP
#define GENERAL_HPP
#include <sys/time.h>
#include <time.h>
#define CACHE_LINE_SIZE 64
#define PRINT_RATE 1000                     //defined in msec

struct tuple_t{
	union
	{
		struct{
			int id;					//tuple id, starting from 0
			int sid;				//sensor id
			double timestamp_usec;	//timestamp converted in usec. It is the time at which the generator will send the tuple (i.e. if accellerated will be different from the original one)
			int x,y,z;				//sensor coordinates
			int v, a;				//velocity and acceleration
			int vx,vy,vz;			//direction vector
			int ax,ay,az;			//acceleration vector
		};
		char padding[CACHE_LINE_SIZE];
	};

};


/**
 * @brief The point_t struct data type passed to the pattern
 * (we need just the coordinates and an indication of the centroid to which the point belongs)
 */
struct point_t{
	//union
	//{
		//struct{
			int id;					//tuple id, starting from 0
			int sid;				//sensor id
			double timestamp_usec;	//timestamp converted in usec
			double x,y,z;				//sensor coordinates
			int centroid_id;
		//};
		//char padding[CACHE_LINE_SIZE];
	//};
};

/**
 * @brief The results_t struct represents the result of the computation
 * We are requiring to compute 3 cluster and it will returns the x,y coordinates
 *
 */
struct results_t{

	int id;			//computed by the function: consecutive windows must have consecutive ids
	double timestamp;
	point_t centroids[3];

};


/**
	Timing functions
*/
inline long current_time_usecs() __attribute__((always_inline));
inline long current_time_usecs(){
	struct timeval t;
	gettimeofday(&t, NULL);
	return (t.tv_sec)*1000000L + t.tv_usec;

}

inline long current_time_nsecs() __attribute__((always_inline));
inline long current_time_nsecs(){
	struct timespec t;
	clock_gettime(CLOCK_REALTIME, &t);
	return (t.tv_sec)*1000000000L + t.tv_nsec;
}
#endif // GENERAL_HPP
