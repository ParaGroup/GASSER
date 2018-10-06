#ifndef GPU_CB_WINDOW_HPP
#define GPU_CB_WINDOW_HPP
#include <vector>
#include <cassert>
#include "I_GPU_Window.hpp"

/* This file provides the following classes:
 * GPU_CB: it is a sort of container for Batched Windows. It essentially maintains the data
 *	of B consecutive Count Based Windows. The triggering is valid only when all the B windows are completed
 *
 * The data is maintained over a std::vector container. For each window are maintained the indices representing
 * the start and the end of the data. They are returned using the get_indexes() method
 *
 * **ATTENTION**: new elements can be added only when the computation on previous windows is completed
 *

 * ***************************************************************************
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
 * Author: Tiziano De Matteis <dematteis at di.unipi.it>
 */

/*!
 *  \class GPU_CB_Window
 *  \ingroup high_level_patterns
 *
 *  \brief Maintains B Count-based Windows with a std::vector Container
 *

 */
template<typename in_tuple_t>
class GPU_CB_Window: public I_GPU_Window<in_tuple_t, std::vector<in_tuple_t>> {
public:
	typedef std::vector<in_tuple_t> win_container_t; // public type of the window container

private:
	unsigned long _win_length; // length of the single window
	unsigned long _win_slide; // slide of the single window
	unsigned long _batch_size;		//number of windows to maintain
	unsigned long _global_size;	//number of elements that have to be received to fill B windows
	win_container_t _win_container; // window container
	unsigned long _expiring_slide;
	std::vector<std::pair<unsigned long,unsigned long>> _win_indexes;
	std::vector<std::pair<unsigned long, unsigned long>> _partial_indexes;


public:
	/**
	 * \brief constructor
	 *
	 * \param wlen the length of the window
	 * \param wslide the sliding factor of the window
	 * \param b number of windows in batch
	 * \param expiring_slide how many tuple expire every after triggering (it should be equal to wslide*b*par_degree)
	 *
	 *
	 * NOTE: here we need the user defined slide to properly dimension the buffer
	 */
	GPU_CB_Window(unsigned long wlen, unsigned long wslide, unsigned long b, unsigned long expiring_slide): _win_length(wlen), _win_slide(wslide), _batch_size(b), _expiring_slide(expiring_slide) {
		assert(wslide <= wlen); // we support sliding or tumbling windows (not hopping windows)!
		//compute the space required to maintain B consecutive windows
		_global_size=_win_slide*(_batch_size-1) + _win_length;
		_win_container.reserve(_global_size); // reserve space (capacity) for window size + window slide tuples

		//in a CB window we can statically here we build the indexes representing the different windows
		//first window always start at 0
		unsigned long idx=0;

		for(int i=0;i<_batch_size;i++)
		{
			_win_indexes.push_back(std::make_pair(idx,idx+_win_length));
			idx+=_win_slide;
		}

	}

	// expire old tuples
	unsigned long expire() {
		// remove and destroy all the unecessary tuples
		if(_expiring_slide>_global_size)
			_win_container.clear();
		else{
				_win_container.erase(_win_container.begin(),_win_container.begin()+_expiring_slide);
		}
		return _expiring_slide;
	}


	/*
	 * Change the expiring slide only. Will be used if we change the number of workers
	 */
	void change_expiring_slide(int new_expiring_slide)
	{
		//for a window the expiring_slide is given
		_expiring_slide=new_expiring_slide;
	}


	/*
	 * Change the batch size: this will require to clear the current window and to change its size and slide
	 */
	void change_batch_size(int new_batch_size, int par_degree)
	{
		_batch_size=new_batch_size;
		_global_size=_win_slide*(_batch_size-1) + _win_length;
		//update expiring slide
		_expiring_slide=_batch_size*_win_slide*par_degree;
		_win_indexes.clear();
		unsigned long idx=0;
		for(int i=0;i<_batch_size;i++)
		{
			_win_indexes.push_back(std::make_pair(idx,idx+_win_length));
			idx+=_win_slide;
		}
		this->reset();
	}

	// insert a new tuple into the window. It returns true if the window is triggered, false otherwise
	// ATTENTION: new elements can be added only when the computation on previous windows is completed
	bool insert(const in_tuple_t& tuple) {

		//if the window is full it means we have already computed the result

		if(_win_container.size()==_global_size)
			this->expire();

		// add the new tuple in the window
		_win_container.push_back(tuple);


		if(_win_container.size() == _global_size) //windows are triggered
			return true;
		else
			return false;
	}

	// return the window content
	win_container_t& get_content() {
		return _win_container;
	}

	// return the size of the window
	unsigned long get_size() const {
		return _win_container.size();
	}

	//return the indexes representing the boundaries of the different windows
	std::vector<std::pair<unsigned long, unsigned long>>& get_indexes()
	{
		return _win_indexes;
	}

	// return the index representing the boundaries of the different windows accumulated till this moment
	//(it is useful on end of stream)
	std::vector<std::pair<unsigned long, unsigned long>>& get_partial_indexes()
	{
		unsigned long idx=0;
		_partial_indexes.clear();
		while(idx+_win_length<=_win_container.size())
		{
			_partial_indexes.push_back(std::make_pair(idx,idx+_win_length));
			idx+=_win_slide;
		}
		return _partial_indexes;

	}



	// empty the entire window content
	unsigned long reset() {

		unsigned long s = this->get_size();
		_win_container.clear();
		return s;
	}
};
#endif // GPU_CB_WINDOW_HPP
