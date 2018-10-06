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

#ifndef BASIC_WINDOW_H
#define BASIC_WINDOW_H

#include <vector>
#include <cassert>
#include "I_Window.hpp"

/* This file provides the following classes:
 *  Basic_Window is the count-based window implementation with a std:vector container
 */

template<typename in_tuple_t>
class Basic_Window: public I_Window<in_tuple_t, std::vector<in_tuple_t>> {
public:
    typedef std::vector<in_tuple_t> win_container_t; // public type of the window container

private:
    unsigned long win_length; // length of the window
    unsigned long win_slide; // slide of the window
    win_container_t win_container; // window container

public:
    /**
     * \brief constructor
     * 
     * \param _wlen the length of the window
     * \param _wslide the sliding factor of the window
     * 
     */
    Basic_Window(unsigned long _wlen, unsigned long _wslide): win_length(_wlen), win_slide(_wslide) {
        assert(_wslide <= _wlen); // we support sliding or tumbling windows (not hopping windows)!
    	win_container.reserve(win_slide + win_length); // reserve space (capacity) for window size + window slide tuples
    }

    // expire old tuples
    unsigned long expire() {
        // remove and destroy the win_slide oldest elements from the container
	   /* for(int i=0; i<win_slide; i++) {
            // remove the tuple
            win_container.erase(win_container.begin());
		}*/
		win_container.erase(win_container.begin(),win_container.begin()+win_slide);
        return win_slide;
    }

    // insert a new tuple into the window. It returns true if the window is triggered, false otherwise
    bool insert(const in_tuple_t& tuple) {
        // add the new tuple in the window
        win_container.push_back(tuple);
        // remove expired elements only if we have win_length + win_slide tuples
        if(win_container.size() == win_length + win_slide) {
            this->expire();
        }
        if(win_container.size() == win_length) return true;
        else return false;
    }

    // return the window content
    win_container_t& get_content() {
    	return win_container;
    }

    // return the size of the window
    unsigned long get_size() const {
        return win_container.size();
    }

    // empty the entire window content
    unsigned long reset() {
        unsigned long s = this->get_size();
        win_container.clear();
        return s;
    }
};

#endif
