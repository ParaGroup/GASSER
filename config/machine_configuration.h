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
 ****************************************************************************
 ****************************************************************************
 *
 * Author: Tiziano De Matteis <dematteis at di.unipi.it>
 * /

/*
 * This file contains the main details regarding the execution machine (GPU characteristics, CPU ...)
 * Please, fill it accordingly to the spec of your own machine
 */


#ifndef MACHINE_CONFIG_H
#define MACHINE_CONFIG_H
/*
 * GPU SPECS
 */
//Number of the streaming multiprocessor present in the GPU
#define SM 15
//Number of cuda core per streaming multiprocessor
#define CORE_PER_SM 192

/*
 * CPU SPECS
 */
//Maximum number of replicas to use. Being N the number of physical core in the machine, the number of replicas should be N-6.
//Higher number can be used but may prevent the overall performances

//Why N-6? Because in the implemented queries we added a certain number of additional entities in order to have a more realistic scenario.
//Indeed, apart from the replicas, distributor, collector and manager there are also the data generator,
//a first stage that perform filtering (at the moment being it just passes all the tuples) and a last stage that computes metrics such as
//the latency and the throughput (optionally, it may save the results on file).

#define REPLICA	10

#endif
