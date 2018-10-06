
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

#ifndef I_WINDOW_H
#define I_WINDOW_H
#include <vector>


/* This file provides the following classes:
 *  I_GPU_Window is the abstract class of a batched window data structure
 */

/*
 *  This class defines the interface of a generic window data structure used for the GPU Based implementation
 */
template<typename in_tuple_t, typename win_container_t>
class I_GPU_Window {
public:
	// delete the expired tuples from the window content
	virtual unsigned long expire() = 0;
	// insert a new tuple in the window, return true if the window has been triggered or false otherwise
	virtual bool insert(const in_tuple_t& tuple) = 0;
	// return the actual number of tuples in the window
	virtual unsigned long get_size() const = 0;
	// return the actual window container
	virtual win_container_t& get_content() = 0;
	// empty the window content
	virtual unsigned long reset() = 0;
	//get start/end indexes of the windows contained
	virtual std::vector<std::pair<unsigned long, unsigned long>>& get_indexes() =0;
	//get start/end indexes of the windows contained till a given moment
	virtual std::vector<std::pair<unsigned long, unsigned long>>& get_partial_indexes()	=0;
	//change the expiring slide (needed for changing the configuration)
	virtual void change_expiring_slide(int new_slide) =0;

};

#endif
