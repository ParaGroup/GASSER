
/*! 
 *  \file I_Window.hpp
 *  \ingroup high_level_patterns
 *  \brief Abstract Class of the Generic Window Data Structure
 *  
 *  This file contains the definition of the abstract class of a window
 *  data structure.
 *  
 *  It requires C++11 standard.
 *  
 */

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
 */

#ifndef I_WINDOW_H
#define I_WINDOW_H

/* This file provides the following classes:
 *  I_Window is the abstract class of a window data structure
 */


template<typename in_tuple_t, typename win_container_t>
class I_Window {
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
};

#endif
