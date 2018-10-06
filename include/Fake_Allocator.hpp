
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

#ifndef FAKE_ALLOCATOR_H
#define FAKE_ALLOCATOR_H

/* This file provides the following classes:
 *  Fake_Allocator is a wrapper to a fake allocator that statically allocates memory space and reuses it
 */
#define FAKE_ENTRIES 50000000

/*! 
 *  \class Win_Allocator
 *  \ingroup high_level_patterns
 *  
 *  \brief Wrapper to a fake allocator
 *  
 *  This class provides a wrapper to a fake allocator that reuses (in a circular manner)
 *  a preallocated array of elements.
 *  
 *  This class is defined in \ref HLPP_patterns/include/Fake_Allocator.hpp
 */
template<typename type_t>
class Fake_Allocator {
private:
	typedef struct wrapper<type_t> wrapper_t;
	type_t** poolsTuples; // pools for the tuples
	wrapper_t** poolsWrappers; // pools for the wrapper
	unsigned int sizePool = FAKE_ENTRIES; // size of each pool (in terms of tuples or wrappers)
	unsigned int numPools = ceil(FAKE_ENTRIES / sizePool); // number of pools
	unsigned int actualPoolTuple;
	unsigned int actualPoolWrapper;
	unsigned int nextTuple;
	unsigned int nextWrapper;

public:
	// constructor
	Fake_Allocator(): actualPoolTuple(0), actualPoolWrapper(0), nextTuple(0), nextWrapper(0) {
		poolsTuples = (type_t**) calloc(numPools, sizeof(type_t*));
		poolsWrappers = (wrapper_t**) calloc(numPools, sizeof(wrapper_t*));
		for(int i = 0; i < numPools; i++) {
			poolsTuples[i] = (type_t*) calloc(sizePool, sizeof(type_t));
			poolsWrappers[i] = (wrapper_t*) calloc(sizePool, sizeof(wrapper_t));
		}
	}

	// destructor
	~Fake_Allocator() {
		for(int i = 0; i < numPools; i++) {
			free(poolsTuples[i]);
			free(poolsWrappers[i]);
		}
		free(poolsTuples);
		free(poolsWrappers);
	}

	// allocate method (item)
	type_t* allocate(std::size_t n) {
		type_t* p = &(poolsTuples[actualPoolTuple][nextTuple]);
		nextTuple++;
		if(nextTuple == sizePool) {
			nextTuple=0;
			actualPoolTuple = (actualPoolTuple + 1) % numPools;
		}
		return p;
	}

	// allocate method (wrapper)
	wrapper_t* allocate_wrapper(std::size_t n) {
		wrapper_t* p = &(poolsWrappers[actualPoolWrapper][nextWrapper]);
		nextWrapper++;
		if(nextWrapper == sizePool) {
			nextWrapper=0;
			actualPoolWrapper = (actualPoolWrapper + 1) % numPools;
		}
		return p;
	}

	// construct method (item)
	void construct(type_t* p) {
		new(p) type_t(); // placement new with empty constructor
	}

	// construct method (wrapper)
	void construct_wrapper(wrapper_t* p) {
		new(p) wrapper_t(); // placement new with empty constructor
	}

	// construct method (item)
	void construct(type_t* p, const type_t& val) {
		new(p) type_t(val); // placement new with copy constructor
	}

	// construct method (wrapper)
	void construct_wrapper(wrapper_t* p, const wrapper_t& val) {
		new(p) wrapper_t(val); // placement new with copy constructor
	}

	// construct method with a variable number of arguments in the constructor (item)
	template<class U, class... Args>
	void construct(U* p, Args&&... args) {
		new(p) U(std::forward<Args>(args)...); // placement new with input arguments in the constructor
	}

	// destroy method (item)
	void destroy(type_t* p) {
		p->~type_t(); // call the destructor of the item
	}

	// destroy method (wrapper)
	void destroy_wrapper(wrapper_t* p) {
		p->~wrapper_t(); // call the destructor of the wrapper
	}

	// destroy method (item)
	template<class U>
	void destroy(U* p) {
		p->~U(); // call the destructor of the item
	}

	// deallocate method (item)
	void deallocate(type_t* p, std::size_t n) {
		// stub
	}

	// deallocate method (wrapper)
	void deallocate_wrapper(wrapper_t* p, std::size_t n) {
		// stub
	}

	// method to print the statistics of the internal allocator
	void printstats() {
		//stub
	}

	// == operator
    inline bool operator==(const Win_Allocator<type_t>& a) {
    	return true;
    }

    // != operator
    inline bool operator!=(const Win_Allocator<type_t>& a) {
    	return !operator==(a);
    }
};

#endif
