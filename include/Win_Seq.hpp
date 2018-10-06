/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/*! 
 *  \file Win_Seq.hpp
 *  \ingroup high_level_patterns
 *  \brief Sequential Window-based Pattern
 *  
 *  This is the sequential pattern of all the window-based stream processing
 *  computations. It operates on one or more sliding windows depending on the
 *  used context (single keyed or multi keyed streams).
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
 *
 * Authors: Tiziano De Matteis <dematteis at di.unipi.it>
 *	    Gabriele Mencagli <mencagli at di.unipi.it>
 */

#ifndef WIN_SEQ_H
#define WIN_SEQ_H

#include <vector>
#include <memory>
#include <functional>
#include <ff/node.hpp>
#include "Win_Allocators.hpp"

using namespace ff;

/* This file provides the following classes:
 *  Win_Seq is the class implementing the Sequential Window-based Pattern
 *  Win_Seq_Wrapper is the class that implements a wrapper to the standard Win_Seq (used by the KP and WF patterns)
 */

/*! 
 *  \class Win_Seq
 *  \ingroup high_level_patterns
 *  
 *  \brief Sequential Window-based Pattern
 *  
 *  This is the sequential window-based pattern. For each input tuple it performs
 *  the following actions:
 *  1-identify the key of the current tuple using a user function;
 *  2-insert the tuple in the corresponding window (or create it if it does not exist);
 *  3-check if the window has been triggered and in the case execute a user function.
 *  
 *  An instance of a Win_Seq makes use of two external allocator objects. The first
 *  is an allocator used for the input tuples, the second an allocator that will be
 *  used to allocate the output results by the pattern. Their types are specified as
 *  template arguments.
 *  
 *  This class is defined in \ref HLPP_patterns/include/Win_Seq.hpp
 */
template<typename IN_T, typename OUT_T, typename WIN_T, typename Ext_Alloc_IN_T=std::allocator<IN_T>, typename Ext_Alloc_OUT_T=std::allocator<OUT_T>>
class Win_Seq: public ff_node_t<IN_T, OUT_T> {
private:
    typedef IN_T in_tuple_t;
    typedef OUT_T out_tuple_t;
    typedef WIN_T win_type_t;
    typedef typename win_type_t::win_container_t win_container_t;
    typedef std::function<std::pair<unsigned int, unsigned long>(const in_tuple_t&)> FUNC_IN_T; // function to retrieve the key id and tuple id from an input tuple
    typedef std::function<void(win_container_t&, unsigned long, out_tuple_t&)> FUNC_F_T; // function business logic on the window content
    FUNC_IN_T M_IN;
    FUNC_F_T F;
    unsigned long win_length;
    unsigned long win_slide;
    std::vector<std::unique_ptr<win_type_t>> winVector; // vector of unique pointers to the window data structures (one per key) used by the Win_Seq
    std::vector<unsigned int> batchVector; // vector of counters used for batching (one per key)
    bool batch_enabled; // this flag is true if batching is enabled
    unsigned long batch_length=0; // length of a batch in number of tuples (meaningful only with batch_enabled equal to true)
    bool isWrapped; // this flag is true if the Win_Seq is used inside a Win_Seq_Wrapper false otherwise
    unsigned int resize_factor; // resize factor used by the internal vectors (winVector and batchVector)
    Ext_Alloc_IN_T* ext_alloc_in; // pointer to an external allocator of input tuples
    Ext_Alloc_OUT_T* ext_alloc_out; // pointer to an external allocator of output results
    bool freeAllocIN=false; // true if the input external allocator have been created into the pattern, false otherwise
    bool freeAllocOUT=false; // true if the output external allocator have been created into the pattern, false otherwise
	//debug and profiling
		long comp_time=0;
		int numb_comp=0;

	int _received=0;
#if defined(PROFILING)
    struct timeval startTAT, endTAT;
    double workerTAT=0;
    long long countTAT=0;
#endif

public:
    /**
     * \brief constructor I
     * 
     * \param _M_IN the function to extract the key and tuple id values from a tuple
     * \param _F the processing function
     * \param _wlen the length of the window
     * \param _wslide the sliding factor of the window
     * \param _ext_alloc_in reference to an external allocator of input tuples (if not specified the type is assumed of a stateless allocator)
     * \param _ext_alloc_out reference to an external allocator of output results (if not specified the type is assumed of a stateless allocator)
     * \param _isWrapped this flag is true if the Win_Seq is used inside a Win_Seq_Wrapper false otherwise
     * \param _resize_factor number of keys initially assumed in the internal vectors (they will be resized by this factor each time, if needed)
     * 
     */
    Win_Seq(FUNC_IN_T _M_IN, FUNC_F_T _F, unsigned long _wlen, unsigned long _wslide, Ext_Alloc_IN_T& _ext_alloc_in, Ext_Alloc_OUT_T& _ext_alloc_out, 
            bool _isWrapped=false, unsigned int _resize_factor=3000)
            :M_IN(_M_IN), F(_F), win_length(_wlen), win_slide(_wslide), winVector(_resize_factor), batchVector(_resize_factor), batch_enabled(false), isWrapped(_isWrapped), resize_factor(_resize_factor),
            ext_alloc_in(&_ext_alloc_in), ext_alloc_out(&_ext_alloc_out) {

        assert(win_slide <= win_length); // we support sliding or tumbling windows (not hopping windows)!
#if defined(PROFILING)    
        gettimeofday(&startTAT, 0);
#endif
    }

    /**
     * \brief constructor II
     * 
     * \param _M_IN the function to extract the key and tuple id values from a tuple
     * \param _F the processing function
     * \param _wlen the length of the window
     * \param _wslide the sliding factor of the window
     * \param _isWrapped this flag is true if the Win_Seq is used inside a Win_Seq_Wrapper false otherwise
     * \param _resize_factor number of keys initially assumed in the internal vectors (they will be resized by this factor each time, if needed)
     *
     */
    Win_Seq(FUNC_IN_T _M_IN, FUNC_F_T _F, unsigned long _wlen, unsigned long _wslide, bool _isWrapped=false, unsigned int _resize_factor=3000)
            :M_IN(_M_IN), F(_F), win_length(_wlen), win_slide(_wslide), winVector(_resize_factor), batchVector(_resize_factor), batch_enabled(false), isWrapped(_isWrapped), resize_factor(_resize_factor) {

        ext_alloc_in = new Ext_Alloc_IN_T();
        ext_alloc_out = new Ext_Alloc_OUT_T();
        freeAllocIN=true;
        freeAllocOUT=true;
    }

    /**
     * \brief constructor III (used by Win_Seq_Wrapper)
     * 
     * \param _M_IN the function to extract the key and tuple id values from a tuple
     * \param _F the processing function
     * \param _wlen the length of the window
     * \param _wslide the sliding factor of the window
     * \param _ext_alloc_out reference to an external allocator of output results (if not specified the type is assumed of a stateless allocator)
     * \param _resize_factor number of keys initially assumed in the internal vectors (they will be resized by this factor each time, if needed)
     *
     */
    Win_Seq(FUNC_IN_T _M_IN, FUNC_F_T _F, unsigned long _wlen, unsigned long _wslide, Ext_Alloc_OUT_T& _ext_alloc_out, unsigned int _resize_factor=3000)
            :M_IN(_M_IN), F(_F), win_length(_wlen), win_slide(_wslide), winVector(_resize_factor), batchVector(_resize_factor), batch_enabled(false), isWrapped(true), ext_alloc_out(&_ext_alloc_out), resize_factor(_resize_factor) {}

    // destructor
    ~Win_Seq() {
        if(freeAllocIN) delete ext_alloc_in;
        if(freeAllocOUT) delete ext_alloc_out;
    }

    // svc_init method
    int svc_init() {
        // initialization of the internal vectors
        for(int i = 0; i < resize_factor; i++) {
            winVector[i] = nullptr; // the window is unitialized
            batchVector[i] = 0;
        }
        return 0;
    }

    // svc method
    out_tuple_t* svc(in_tuple_t* in) {


        std::pair<unsigned int, unsigned long> in_info = M_IN(*in);
        unsigned int key = in_info.first; // extract the key
        unsigned long id = in_info.second; // extract the id of the tuple
        // check if the key is in the actual vectors, otherwise resize them

		//currently this part is not used since we have only one key
        if (key >= winVector.size()) {
            unsigned int old_size = winVector.size();
            // resize the two vectors
            winVector.resize(key+1);
            batchVector.resize(key+1);
            // initialize the new elements of the two vectors
            for(int i = old_size; i < key+1; i++) {
                winVector[i] = nullptr; // the window is unitialized
                batchVector[i] = 0;
            }
        }
        // check if the window is initiliazed, otherwise initialize it
		if(winVector[key] == nullptr) {
            std::unique_ptr<win_type_t> w(new win_type_t(win_length, win_slide));
			winVector[key] = std::move(w);
        }
        // we have one more tuple for that key
		batchVector[key]++;
        // insert a copy of the tuple in the window and check whether it is triggered or not
		bool triggered = winVector[key]->insert(*in);

        // if Win_Seq is not wrapped we destroy and deallocate the input tuple using the external input allocator
        if(!isWrapped) {
            ext_alloc_in->destroy(in);
            ext_alloc_in->deallocate(in, 1);
        }

		_received++;
        // if not triggered go on
    	if(!triggered) {
            return ff_node_t<in_tuple_t, out_tuple_t>::GO_ON;
        }
        // else compute the window
        else {
			long start_usec=ff::getusec();
            // allocate and construct the output data structure using the external output allocator
			out_tuple_t* out = nullptr;
			ext_alloc_out->allocate(out,1);
            ext_alloc_out->construct(out);
            // execute the user business logic function
			//if(_received<5005)
				//printf("[%d] %.1f\t%.1f\t\n", (winVector[key-1])->get_size(),((winVector[key-1])->get_content())[0].x,((winVector[key-1])->get_content())[0].y);

			F((winVector[key])->get_content(), (winVector[key])->get_size(), *out);

            // if batching is enabled and we have reached the number of tuples in a batch
			if((batch_enabled) && (batchVector[key] == batch_length)) {
				winVector[key]->reset(); // empty window content
				batchVector[key] = 0; // reset the batch counter
            }
			comp_time+=ff::getusec()-start_usec;
			numb_comp++;
			return out;
        }
    }

    // svc_end method
    void svc_end() {
#if (defined(PROFILING) && !defined(SCRIPT_MODE))
        printf("Win_Seq: average inter-arrival time per triggering tuple: %f usec\n", workerTAT);
#endif
		printf("Number of computation: %d avg. comp. time: %.3f\n",numb_comp,((double)comp_time)/numb_comp);

    }

    // run method
    virtual int run(bool = false) {
        return ff_node::run();
    }

    // wait method
    virtual int wait() {
        return ff_node::wait();
    }

    // run_then_freeze method
    virtual int run_then_freeze() {
        if (ff_node::isfrozen()) {
            ff_node::thaw(true);
            return 0;
        }
        return ff_node::freeze_and_run();
    }

    // wait_freezing method
    virtual int wait_freezing() {
        return ff_node::wait_freezing();
    }

    // method to enable batching and set the batch length in number of tuples
    virtual void enable_batch(unsigned long length) {
        this->batch_enabled = true;
        this->batch_length = length;
    }

    // method to disable batching
    virtual void disable_batch() {
        this->batch_length = false;
    }
};

/*! 
 *  \class Win_Seq_Wrapper
 *  \ingroup high_level_patterns
 *  
 *  \brief A Wrapper to the Standard Win_Seq
 *  
 *  This is a wrapper to the standard Win_Seq. The idea is that each input tuple is wrapped into
 *  a wrapper struct. The same for each output result. This is needed by the Win_KP and the Win_WF patterns.
 *  
 *  This class is defined in \ref HLPP_patterns/include/Win_Seq.hpp
 */
template<typename IN_T, typename OUT_T, typename WIN_T>
class Win_Seq_Wrapper: public ff_node_t<wrapper<IN_T>, wrapper<OUT_T>> {
private:
    typedef IN_T  in_tuple_t;
    typedef OUT_T out_tuple_t;
    typedef WIN_T win_type_t;
    typedef struct wrapper<in_tuple_t> tuple_wrapper_t;
    typedef struct wrapper<out_tuple_t> output_wrapper_t;
    typedef typename win_type_t::win_container_t win_container_t;
    typedef std::function<std::pair<unsigned int, unsigned long>(const in_tuple_t&)> FUNC_IN_T; // function to retrieve the key id and tuple id from an input tuple
    typedef std::function<void(win_container_t&, unsigned long, out_tuple_t&)> FUNC_F_T; // function business logic on the window content
    Win_Allocator<out_tuple_t>* int_alloc_out; // pointer to the Win_Allocator of output results
    Win_Seq<in_tuple_t, out_tuple_t, win_type_t, Win_Allocator<in_tuple_t>, Win_Allocator<out_tuple_t>> myworker;
#if defined(PROFILING)
    struct timeval startTS, endTS, startTA, endTA, startTST, endTST, startTSNT, endTSNT;
    double workerTS=0, workerTA=0, workerTST=0, workerTSNT=0;
    long long countTS=0, countTA=0, countTST=0, countTSNT=0;
#endif

public:
/**
     * \brief constructor
     * 
     * \param _M_IN the function to extract the key and tuple id values from a tuple
     * \param _F the processing function of the Workers
     * \param _wlen the length of the windows
     * \param _wslide the sliding factor of the windows
     * \param _int_alloc_out pointer to the Win_Allocator of output results
     *
     */
    Win_Seq_Wrapper(FUNC_IN_T _M_IN, FUNC_F_T _F, unsigned long _wlen, unsigned long _wslide,
            Win_Allocator<out_tuple_t>* _int_alloc_out=new Win_Allocator<out_tuple_t>(MAGIC_CONSTANT_OUT))
            :myworker(_M_IN, _F, _wlen, _wslide, *(_int_alloc_out)), int_alloc_out(_int_alloc_out) {

#if defined(PROFILING)
        gettimeofday(&startTA, 0);
#endif
    }

	/*// destructor
    ~Win_Seq_Wrapper() {
        delete int_alloc_out;
	}*/

    // svc method
    output_wrapper_t* svc(tuple_wrapper_t* wrapper_in) {
#if defined(PROFILING)
        gettimeofday(&endTA, 0);
        startTS = endTA;
        startTST = endTA;
        startTSNT = endTA;
        workerTA = workerTA * countTA;
        double elapsedTime = (endTA.tv_sec - startTA.tv_sec) * 1000000.0;
        elapsedTime += (endTA.tv_usec - startTA.tv_usec);
        workerTA += elapsedTime;
        countTA++;
        workerTA = workerTA / countTA;
        gettimeofday(&startTA, 0);
#endif
        in_tuple_t* tuple = wrapper_in->item;
        // compute the possible result by invoking the svc the real worker     
        out_tuple_t* result = myworker.svc(tuple);
        // atomically decrement the tuple's reference counter
        int old_cnt = (wrapper_in->counter).fetch_sub(1);
        // if I am the last, I can destroy the tuple
        if(old_cnt == 1) {
            Win_Allocator<in_tuple_t>* allocator = wrapper_in->allocator;
            // destroy and deallocate the tuple with the internal allocator in the wrapper
            allocator->destroy(tuple);
			//allocator->deallocate(tuple, 1);
            // destroy and deallocate the wrapper with the internal allocator in the wrapper
			allocator->destroy(wrapper_in);
			allocator->deallocate_all(wrapper_in, 1);
        }
        if(result == GO_ON) {
#if defined(PROFILING)
            gettimeofday(&endTS, 0);
            endTSNT = endTS;
            workerTS = workerTS * countTS;
            double elapsedTime = (endTS.tv_sec - startTS.tv_sec) * 1000000.0;
            elapsedTime += (endTS.tv_usec - startTS.tv_usec);
            workerTS += elapsedTime;
            countTS++;
            workerTS = workerTS / countTS;
            workerTSNT = workerTSNT * countTSNT;
            elapsedTime = (endTSNT.tv_sec - startTSNT.tv_sec) * 1000000.0;
            elapsedTime += (endTSNT.tv_usec - startTSNT.tv_usec);
            workerTSNT += elapsedTime;
            countTSNT++;
            workerTSNT = workerTSNT / countTSNT;
#endif
            return ff_node_t<tuple_wrapper_t, output_wrapper_t>::GO_ON;
        }
        else {
            // allocate and construct the wrapper
			output_wrapper_t* wrapper_out = nullptr;
			int_alloc_out->allocate(wrapper_out,1);
			int_alloc_out->construct(wrapper_out);
            // set the wrapper parameters
            wrapper_out->counter = 1; // not meaningful
            wrapper_out->item = result;
            wrapper_out->allocator = int_alloc_out;
#if defined(PROFILING)
            gettimeofday(&endTS, 0);
            endTST = endTS;
            workerTS = workerTS * countTS;
            double elapsedTime = (endTS.tv_sec - startTS.tv_sec) * 1000000.0;
            elapsedTime += (endTS.tv_usec - startTS.tv_usec);
            workerTS += elapsedTime;
            countTS++;
            workerTS = workerTS / countTS;
            workerTST = workerTST * countTST;
            elapsedTime = (endTST.tv_sec - startTST.tv_sec) * 1000000.0;
            elapsedTime += (endTST.tv_usec - startTST.tv_usec);
            workerTST += elapsedTime;
            countTST++;
            workerTST = workerTST / countTST;
#endif
            return wrapper_out;
        }
    }

    // svc_end method
    void svc_end() {
#if !defined(SCRIPT_MODE)
#if defined(PROFILING)
        printf("****************************************************************************************\n");
        printf("Win_Seq: average service time: %f usec\n", workerTS);
        printf("Win_Seq: average inter-arrival time: %f usec\n", workerTA);
        printf("Win_Seq: average service time per triggering tuple: %f usec\n", workerTST);
        printf("Win_Seq: average service time per non-triggering tuple: %f usec\n", workerTSNT);
        printf("****************************************************************************************\n");
#endif
        myworker.svc_end();
#if defined(ALLOCATOR_STATS)
        // print the statistics of the FF allocator used to allocate output results
        int_alloc_out->printstats();
#endif
#endif
    }

    // method to enable batching and set the batch length in number of tuples
    virtual void enable_batch(unsigned long length) {
        myworker.enable_batch(length);
    }

    // method to disable batching
    virtual void disable_batch() {
        myworker.disable_batch();
    }
};

#endif
