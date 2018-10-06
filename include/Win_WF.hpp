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
 *Authors: Tiziano De Matteis <dematteis at di.unipi.it>
 *	    Gabriele Mencagli <mencagli at di.unipi.it>
 * /
 */

#ifndef WIN_WF_H
#define WIN_WF_H

#include <tuple>
#include <vector>
#include <queue>
#include <time.h>
#include <math.h>
#include <iostream>
#include <functional>
#include <ff/node.hpp>
#include <ff/farm.hpp>
#include <ff/pipeline.hpp>
#include "Win_Seq.hpp"
#include "Win_Allocators.hpp"

const size_t PER_WORKER_INPUT_QUEUE_LEN  = 262144;
const size_t PER_WORKER_OUTPUT_QUEUE_LEN =   2048;


using namespace ff;

/* This file provides the following classes:
 *  Win_WF is the class implementing the Window Farming Pattern
 */

/*! 
 *  \class Win_WF
 *  \ingroup high_level_patterns
 *  
 *  \brief The Window Farming (WF) Window-based Pattern
 *  
 *  The WF pattern is an extension of the classic farm skeleton. It can be seen
 *  as a 3-stages pipeline. The first stage is the \a Emitter, which distributes
 *  tuples to the Workers. The distribution is performed in such a way that
 *  multiple windows of the same key (substream) are routed to different workers.
 *  If windows have overlapped regions, the same tuple can be trasmitted to different
 *  workers. Workers represent the second stage. Each Worker performs the computation on a set of
 *  windows. The last stage is the \a Collector (\ref ff_gatherer "gt_t") that
 *  gathers the results computed by the \a Workers. Since output results of the same
 *  logical substream must be emitted in the proper order, the collector is also in
 *  charge of applying a partial ordering among the results of the same key attribute.
 *  
 *  An instance of a Win_WF makes use of two external allocator objects. The first
 *  is an allocator used for the input tuples, the second an allocator that will be
 *  used to allocate the output results by the pattern. Their types are specified
 *  as template arguments.
 *  
 *
 *	PLEASE NOTE: tuples must start with id=1!
 *
 *
 *  This class is defined in \ref HLPP_patterns/include/Win_WF.hpp
 *
 * Author: Gabriele Mencagli <mencagli at di.unipi.it>
 */
template<typename IN_T, typename OUT_T, typename WIN_T, typename Ext_Alloc_IN_T=std::allocator<IN_T>, typename Ext_Alloc_OUT_T=std::allocator<OUT_T>>
class Win_WF: public ff_farm<> {
private:
    typedef IN_T in_tuple_t;
    typedef OUT_T out_tuple_t;
    typedef WIN_T win_type_t;
    typedef struct wrapper<in_tuple_t> tuple_wrapper_t;
    typedef struct wrapper<out_tuple_t> output_wrapper_t;
    typedef std::function<std::pair<unsigned int, unsigned long>(const in_tuple_t&)> FUNC_IN_T; // function to retrieve the key id and tuple id from an input tuple
    typedef std::function<std::pair<unsigned int, unsigned long>(const out_tuple_t&)> FUNC_OUT_T; // function to retrieve the key id and result if from an output result
    typedef typename win_type_t::win_container_t win_container_t;
    typedef std::function<void(win_container_t&, unsigned long, out_tuple_t&)> FUNC_F_T; // function business logic on the window content
	unsigned int pardegree; // parallelism degree
    bool batch_enabled; // this flag is true if batching is enabled
    Ext_Alloc_IN_T* ext_alloc_in; // pointer to an external allocator of input tuples
    Ext_Alloc_OUT_T* ext_alloc_out; // pointer to an external allocator of output results
    bool freeAlloc=false; // true if the external allocators have been created into the pattern, false otherwise

public:
    /*!
    *  \internal WF_Emitter
    *  
    *  \brief The Emitter of the Window Farming Pattern
    *  
    *  This is the Emitter of the Window Farming pattern that distrubutes
    *  each input tuple to one or more workers in such a way that the windows are distributed
    *  to the workers according to batch size. The same tuple can be transmitted
    *  to multiple workers. This can be performed by copying it or sharing the tuple
    *  depending on the flag passed to the constructor.
    *  
    *  This inner class is defined in \ref HLPP_patterns/include/Win_WF.hpp
    */
    class WF_Emitter: public ff_node_t<in_tuple_t, tuple_wrapper_t> {
    private:
        FUNC_IN_T M_IN; // function to retrieve the key id and tuple id from an input tuple
        unsigned long win_length;
        unsigned long win_slide;
        unsigned long batch_size;
        unsigned int pardegree; // number of workers
        ff_loadbalancer* const lb;
        Ext_Alloc_IN_T* ext_alloc_in; // pointer to the external allocator of input tuples
		Win_Allocator<in_tuple_t> *int_alloc_in; // internal allocator of input tuples (one for all the workers)
        unsigned long totalReceived=0; // total number of tuples received
        unsigned long totalCopies=0; // total number of copies for all the tuples received
        unsigned long wb; // window length in terms of batches
        unsigned long sb; // window slide in terms of batches
        unsigned int* to_workers; // identifiers of the workers that will receive the input tuple (wrappered)

    public:
        // constructor
		WF_Emitter(FUNC_IN_T _M_IN, long _wlen, long _wslide, long _batch_size, unsigned int _nw, ff_loadbalancer* const _lb, Ext_Alloc_IN_T* _ext_alloc_in, int allocator_kind)
				   :M_IN(_M_IN), win_length(_wlen), win_slide(_wslide), batch_size(_batch_size), pardegree(_nw), lb(_lb), ext_alloc_in(_ext_alloc_in), int_alloc_in(nullptr){
            to_workers = new unsigned int[pardegree];
            wb = win_length + (batch_size - 1) * win_slide;
            sb = win_slide * batch_size;

			if (allocator_kind == 0) {// standard
				printf("EMITTER Using the standard allocator\n");
				int_alloc_in = new Win_Allocator<in_tuple_t>();
			} else if (allocator_kind == 1) { // TBB
				printf("EMITTER Using the TBB is not allowed\n");
				//int_alloc_in = new Win_AllocatorTBB<in_tuple_t>();
				abort();
			} else if (allocator_kind == 2) { // FF
				printf("EMITTER Using the FF allocator (magic=%ld)\n", (PER_WORKER_INPUT_QUEUE_LEN/2)*_nw);
				int_alloc_in = new Win_AllocatorFF<in_tuple_t>((PER_WORKER_INPUT_QUEUE_LEN/2)*_nw);
			} else {
				printf("EMITTER Using the STATIC allocator (magic=%ld)\n", (PER_WORKER_INPUT_QUEUE_LEN) * _nw);
				int_alloc_in = new Win_StaticAllocator<in_tuple_t>((PER_WORKER_INPUT_QUEUE_LEN) * _nw);
			}

        }

        // destructor
        ~WF_Emitter() {
            delete[] to_workers;
        }

        // svc method
        tuple_wrapper_t* svc(in_tuple_t* in) {

			//TODO: sistemare questa cosa delle chiavi


//			if(totalReceived<3)
//			{
//				printf("- %.1f\t%.1f\t%.1f\n",in->x,in->y,in->z);
//			}
            // IMPORTANT: window and tuples ids must start from 1!!!
            std::pair<int,long> info = M_IN(*in); // obtain from the tuple the id of the key and the id of the tuple
            unsigned int id_key = info.first; // first is the id of the key the tuple belongs to
            unsigned long id_tuple = info.second; // second is the id of the tuple
            // determine the set of batches (windows if batch=1) the tuple belongs

            unsigned long first_id;
			if(id_tuple < wb) first_id = 0;
			else first_id = floor(((double) (id_tuple - wb))/((double) sb)) + 1;
			unsigned long last_id = floor(((double) id_tuple)/((double) sb));

            // determine the set of workers that receive the tuple in
            unsigned int countWorkers=0;
            unsigned long i = first_id;
            // the first window of key id_key is assigned to worker startWorkerId
		   // unsigned int startWorkerId = (id_key-1)%pardegree; //for the moment being we are not considering window
			unsigned int startWorkerId = 0;
            while((i<=last_id) && (countWorkers<pardegree)) {
                //to_workers[countWorkers] = (i-1)%pardegree; // very long transient phase with bounded queues
				to_workers[countWorkers] = (startWorkerId + i)%pardegree; // short transient phase with bounded queues
                countWorkers++;
                i++;
            }
            // update the statistics for the number of copies per tuple
            totalCopies+=countWorkers;
            totalReceived++;

            // allocate and construct the tuple
			tuple_wrapper_t* wrapper_in =nullptr;
			int_alloc_in->allocate_all((void*&)wrapper_in, 1);
			int_alloc_in->construct(wrapper_in);

			in_tuple_t* elem = (in_tuple_t*)((char *)wrapper_in + sizeof(tuple_wrapper_t));

			//int_alloc_in->allocate(elem, 1);
			int_alloc_in->construct(elem, *in); // THIS REQUIRES THAT in_tuple_t MUST HAVE A COPY CONSTRUTOR!!!!

			// set the wrapper parameters
			wrapper_in->counter = countWorkers;
			wrapper_in->item = elem;
			wrapper_in->allocator = int_alloc_in;


			/*in_tuple_t* elem = int_alloc_in.allocate(1);
            int_alloc_in.construct(elem, *in); // THIS REQUIRES THAT in_tuple_t MUST HAVE A COPY CONSTRUTOR!!!!
            // allocate and construct the wrapper
            tuple_wrapper_t* wrapper_in = int_alloc_in.allocate_wrapper(1);
            int_alloc_in.construct_wrapper(wrapper_in);
            // set the wrapper parameters
            wrapper_in->counter = countWorkers;
            wrapper_in->item = elem;
			wrapper_in->allocator = &int_alloc_in;*/
            // for each destination we send the same wrapper of the input tuple
			for(int i = 0; i < countWorkers; i++) {
				while(!lb->ff_send_out_to(wrapper_in, to_workers[i]));
            }
            // destroy and deallocate the original input element using the external allocator
            ext_alloc_in->destroy(in);
            ext_alloc_in->deallocate(in, 1);
#if defined(PROFILING)
            gettimeofday(&end, 0);
            timeEmitter = timeEmitter * countEmitter;
            double elapsedTime = (end.tv_sec - start.tv_sec) * 1000000.0;
            elapsedTime += (end.tv_usec - start.tv_usec);
            timeEmitter += elapsedTime;
            countEmitter++;
            timeEmitter = timeEmitter / countEmitter;
            gettimeofday(&start, 0);
#endif
            return ff_node_t<in_tuple_t, tuple_wrapper_t>::GO_ON;
        }

        // svc_end method
        void svc_end() {
#if !defined(SCRIPT_MODE)
            printf("WF_Emitter: number of send per tuple: %f\n", (double) totalCopies/totalReceived);
#if defined(PROFILING)
            printf("WF_Emitter: average emitter service time: %f usec\n", timeEmitter);
#endif
#if defined(ALLOCATOR_STATS)
            // print statistics of the FF allocator used to allocate input tuples
            int_alloc_in.printstats();
#endif
#endif
        }
    };

    /*!
    *  \internal WF_Collector
    *  
    *  \brief The Collector of the Window Farming Pattern
    *  
    *  This is the Collector of the Window Farming pattern that collects results from the
    *  workers and produces an ordered sequence of results outside. A partial ordering is
    *  applied: all the results with the same key are ordered but a non-deterministic order
    *  is followed by the results belonging to different keys.
    *  
    *  This inner class is defined in \ref HLPP_patterns/include/Win_WF.hpp
    */
    class WF_Collector: public ff_node_t<output_wrapper_t, out_tuple_t> {
    private:
        FUNC_OUT_T M_OUT; // function to retrieve the key id and result id from an output result
        // inner class for the comparisor functor used in the priority queue
        class comparisor {
            FUNC_OUT_T func;
        public:
            comparisor(FUNC_OUT_T _func):func(_func) {}
            bool operator() (const output_wrapper_t a, const output_wrapper_t b) const {
                unsigned long a_id = func(*(a.item)).second;
                unsigned long b_id = func(*(b.item)).second;
                return a_id > b_id;
            }
        };
        std::vector<std::unique_ptr<std::priority_queue<output_wrapper_t, std::vector<output_wrapper_t>, comparisor>>> queueVector;
        std::vector<unsigned int> nextidVector;
        Ext_Alloc_OUT_T* ext_alloc_out; // pointer to the external allocator of output results
        bool isOrdered; // true if the collector is ordered (partial) false otherwise
        unsigned int resize_factor; // resize factor used by the internal vectors (queueVector and nextidVector)

    public:
        // constructor
        WF_Collector(FUNC_OUT_T _M_OUT, Ext_Alloc_OUT_T* _ext_alloc_out, bool _isOrdered, unsigned int _resize_factor=3000)
                     :M_OUT(_M_OUT), queueVector(_resize_factor), nextidVector(_resize_factor), ext_alloc_out(_ext_alloc_out), isOrdered(_isOrdered), resize_factor(_resize_factor) {}

        // svc_init method
        int svc_init() {
            // initialization of the internal vectors
            for(int i = 0; i < resize_factor; i++) {
                queueVector[i] = nullptr; // the queue is unitialized
				nextidVector[i] = 0;		//results id start from zero
            }
            return 0;
        }

        // svc method
        out_tuple_t* svc(output_wrapper_t* wrapper_out) {
            out_tuple_t* result = wrapper_out->item;
            std::pair<unsigned int, unsigned long> result_info = M_OUT(*result);
            unsigned int key = result_info.first; // extract the key
            unsigned long id = result_info.second; // extract the id of the result
            if(!isOrdered) { // the collector is not ordered
                // allocate and construct the to_send result with the external allocator
                out_tuple_t* to_send = ext_alloc_out->allocate(1);
                ext_alloc_out->construct(to_send, *result); // THIS REQUIRES THAT out_tuple_t MUST HAVE A COPY CONSTRUTOR!!!!
                // destroy and deallocate the result with the allocator in the wrapper
                Win_Allocator<out_tuple_t>* allocator = wrapper_out->allocator;
                allocator->destroy(result);
                allocator->deallocate(result, 1);
                // destroy and deallocate the wrapper with the allocator in the wrapper
				allocator->destroy(wrapper_out);
				allocator->deallocate(wrapper_out, 1);
                while(!ff_node_t<output_wrapper_t, out_tuple_t>::ff_send_out(to_send));       
            }
            else { // the collector is ordered
                // check if the key is in the actual vectors, otherwise resize them
				//Currently this part is not used since we have only one key
                if (key >= queueVector.size()) {
                    unsigned int old_size = queueVector.size();
                    // resize the two vectors
                    queueVector.resize(key+1);
                    nextidVector.resize(key+1);
                    // initialize the new elements of the two vectors
                    for(int i = old_size; i < key+1; i++) {
                        queueVector[i] = nullptr; // the queue is unitialized
                        nextidVector[i] = 1;
                    }
                }
                // check if the queue of the key is initiliazed, otherwise initialize it
				if(queueVector[key] == nullptr) {
                    std::unique_ptr<std::priority_queue<output_wrapper_t, std::vector<output_wrapper_t>, comparisor>> q(new std::priority_queue<output_wrapper_t, std::vector<output_wrapper_t>, comparisor>(comparisor(M_OUT)));
					queueVector[key] = std::move(q);
                }
                // if next_id == id then the element can be safely transmitted

				if(nextidVector[key] == id) {
                    // allocate and construct the to_send result with the external allocator
                    out_tuple_t* to_send = ext_alloc_out->allocate(1);
                    ext_alloc_out->construct(to_send, *result); // THIS REQUIRES THAT out_tuple_t MUST HAVE A COPY CONSTRUTOR!!!!
                    // destroy and deallocate the result with the allocator in the wrapper
                    Win_Allocator<out_tuple_t>* allocator = wrapper_out->allocator;
                    allocator->destroy(result);
                    allocator->deallocate(result, 1);
                    // destroy and deallocate the wrapper with the allocator in the wrapper
					allocator->destroy(wrapper_out);
					allocator->deallocate(wrapper_out, 1);
                    while(!ff_node_t<output_wrapper_t, out_tuple_t>::ff_send_out(to_send));
                    // increment next_id
					nextidVector[key]++;
                    // if the queue is not empty, dequeue the priority queue from results with contiguous ids starting from next_id
					if(!queueVector[key]->empty()) {
						output_wrapper_t wrapper_next = queueVector[key]->top();
                        out_tuple_t* next = wrapper_next.item;
						while(M_OUT(*next).second == nextidVector[key]) {
							queueVector[key]->pop(); // remove the output wrapper from the queue
                            // allocate and construct the to_send result with the external allocator
                            out_tuple_t* to_send = ext_alloc_out->allocate(1);
                            ext_alloc_out->construct(to_send, *next); // THIS REQUIRES THAT out_tuple_t MUST HAVE A COPY CONSTRUTOR!!!!
                            // destroy and deallocate the next result with the allocator in the wrapper
                            Win_Allocator<out_tuple_t>* allocator = wrapper_next.allocator;
                            allocator->destroy(next); 
                            allocator->deallocate(next, 1);
                            while(!ff_node_t<output_wrapper_t, out_tuple_t>::ff_send_out(to_send));
                            // increment next_id
							nextidVector[key]++;
							if(queueVector[key]->empty()) break; // if queue is now empty break
                            // extract the next top output descriptor from the queue
							wrapper_next = queueVector[key]->top();
                            next = wrapper_next.item;
                        }
                    }
                }
                // otherwise the element must be buffered
                else {
                    // insert the wrapper in the priority queue (by copying it)
					queueVector[key]->push(*wrapper_out);
                    // destroy and deallocate the wrapper with the allocator in the wrapper
                    Win_Allocator<out_tuple_t>* allocator = wrapper_out->allocator;
					allocator->destroy(wrapper_out);
					allocator->deallocate(wrapper_out, 1);
                }
            }
            return ff_node_t<output_wrapper_t, out_tuple_t>::GO_ON;
        }
    };

    /**
     * \brief constructor I
     * 
     * \param _M_IN the function to extract the key value and tuple id from the input tuple
     * \param _M_OUT the function to extract the key value and tuple id from the output tuple
     * \param _F the processing function of the Workers
     * \param _nw the actual parallelism degree
     * \param _wlen the length of the windows
     * \param _wslide the sliding factor of the windows
     * \param _ext_alloc_in reference to an external allocator of input tuples (if not specified the type must be a stateless allocator)
     * \param _ext_alloc_out reference to an external allocator of output results (if not specified the type must be a stateless allocator)
     * \param input_ch states the presence of input channel
     * \param _batch_enabled flag to enable batching
     * \param ordering flag to disable partial ordering in the collector
     * 
     */
    Win_WF(FUNC_IN_T _M_IN, FUNC_OUT_T _M_OUT, FUNC_F_T _F, unsigned int _nw, unsigned long _wlen, unsigned long _wslide, Ext_Alloc_IN_T& _ext_alloc_in, Ext_Alloc_OUT_T& _ext_alloc_out, 
		   bool input_ch=false, bool _batch_enabled=false, bool _ordering=true, int allocator_kind=0)
#if defined(BOUNDED_QUEUE)
           :ff_farm<>(input_ch, 100000*_nw, 5000*_nw, true, _nw, true), pardegree(_nw), batch_enabled(_batch_enabled),
           ext_alloc_in(&_ext_alloc_in), ext_alloc_out(&_ext_alloc_out)
#else
           :ff_farm<>(input_ch, DEF_IN_BUFF_ENTRIES, DEF_OUT_BUFF_ENTRIES, true, _nw), pardegree(_nw), batch_enabled(_batch_enabled),
           ext_alloc_in(&_ext_alloc_in), ext_alloc_out(&_ext_alloc_out)
#endif
    {
        assert(pardegree>0);
        assert(_wslide <= _wlen); // we support sliding or tumbling windows (not hopping windows)!
        std::vector<ff_node *> w(pardegree);
        // if batch is enabled compute the minimum batch
        unsigned long batch_size = 1; // in number of windows
        unsigned long worker_slide = (_wslide * pardegree) > _wlen ? _wlen : _wslide * pardegree;
        if(batch_enabled) {
            // batch_size is 1 if pardegree is 1
            batch_size = pardegree > 1 ? ceil((float) (_wlen - _wslide) / (float) (_wslide * pardegree - _wslide)) : 1;
            if (batch_size > 1) worker_slide = _wslide;

        }
#if !defined(SCRIPT_MODE)
#if defined(BOUNDED_QUEUE)
        std::cout << "Win_WF executed with bounded queues" << std::endl;
#else
        std::cout << "Win_WF executed with unbounded queues" << std::endl;
#endif
        std::cout << "Batching " << batch_enabled << " batch = " << batch_size << " window(s)" << std::endl;
        if(!_ordering) std::cout << "Standard collector used" << std::endl;
        else std::cout << "Ordering collector used" << std::endl;
#endif

		Win_Allocator<out_tuple_t> * out_allocator = nullptr;

		if (allocator_kind == 0) {// standard
			printf("WORKERS Using the standard allocator\n");
			out_allocator = new Win_Allocator<out_tuple_t>();
		} else if (allocator_kind == 1) { // TBB
			printf("WORKERS Using the TBB is not allowed\n");
			//out_allocator = new Win_AllocatorTBB<out_tuple_t>();
			abort();
		} else if (allocator_kind == 2) { // FF
			printf("WORKERS CANNOT USE FF allocator, SWITCHING TO STATIC, (magic=%ld)\n", (PER_WORKER_OUTPUT_QUEUE_LEN * 2 * _nw));
			out_allocator = new Win_StaticAllocator<out_tuple_t>(PER_WORKER_OUTPUT_QUEUE_LEN * 2 * _nw);
		} else {
			printf("WORKERS Using the STATIC allocator (magic=%ld)\n", (PER_WORKER_OUTPUT_QUEUE_LEN * 2 * _nw));
			out_allocator = new Win_StaticAllocator<out_tuple_t>(PER_WORKER_OUTPUT_QUEUE_LEN * 2 * _nw);
		}


        for(size_t i = 0; i<pardegree; i++) {
			Win_Seq_Wrapper<in_tuple_t, out_tuple_t, win_type_t>* p = new Win_Seq_Wrapper<in_tuple_t, out_tuple_t, win_type_t>(_M_IN, _F, _wlen, worker_slide, out_allocator);
            w[i] = p;
            // if batch_size is greater than one we set the batching parameters of the inner Win_Seq_Wrapper instances
            if(batch_size > 1) {
                unsigned long wb = _wlen + (batch_size - 1) * _wslide;
                p->enable_batch(wb);
            }
        }
        ff_farm<>::add_workers(w); // add workers
        ff_farm<>::add_collector(new WF_Collector(_M_OUT, ext_alloc_out, _ordering)); // add customized collector
		ff_farm<>::add_emitter(new WF_Emitter(_M_IN, _wlen, _wslide, batch_size, pardegree, this->getlb(), ext_alloc_in,allocator_kind)); // add customized emitter
    }

    /**
     * \brief constructor II
     * 
     * \param _M_IN the function to extract the key value and tuple id from the input tuple
     * \param _M_OUT the function to extract the key value and tuple id from the output tuple
     * \param _F the processing function of the Workers
     * \param _nw the actual parallelism degree
     * \param _wlen the length of the windows
     * \param _wslide the sliding factor of the windows
     * \param input_ch states the presence of input channel
     * \param _batch_enabled flag to enable batching
     * \param ordering flag to disable partial ordering in the collector
     * 
     */
    Win_WF(FUNC_IN_T _M_IN, FUNC_OUT_T _M_OUT, FUNC_F_T _F, unsigned int _nw, unsigned long _wlen, unsigned long _wslide, bool input_ch=false, bool _batch_enabled=false, bool _ordering=true)
           :Win_WF(_M_IN, _M_OUT, _F, _nw, _wlen, _wslide, *(new Ext_Alloc_IN_T()), *(new Ext_Alloc_OUT_T()), input_ch, _batch_enabled, _ordering) {
        freeAlloc=true;
    }

    // destructor
    ~Win_WF() {
        if(freeAlloc) {
            delete ext_alloc_in;
            delete ext_alloc_out;
        }
    }

    // method to enable batching and set the batch length in number of tuples
    virtual void enable_batch(unsigned long length) {
        // return the svector of the workers
        const svector<ff_node*>& workers = this->getWorkers();
        // we enable the batch in all the inner patterns
		for(size_t i = 0; i<pardegree; i++) (static_cast<Win_Seq_Wrapper<in_tuple_t, out_tuple_t, win_type_t> *const>(workers[i]))->enable_batch(length);
    }

    // method to disable batching
    virtual void disable_batch() {
        // return the svector of the workers
        const svector<ff_node*>& workers = this->getWorkers();
        // we enable the batch in all the inner patterns
        for(size_t i = 0; i<pardegree; i++) (static_cast<Win_Seq_Wrapper<in_tuple_t, out_tuple_t, win_type_t> *const>(workers[i]))->disable_batch();
    }

    // ------------------------- deleted method ---------------------------
    int add_workers(std::vector<ff_node *> &w)                    = delete;
    int add_emitter(ff_node * e)                                  = delete;
    int add_collector(ff_node * c)                                = delete;
    bool load_result(void ** task,
                     unsigned long retry=((unsigned long)-1),
                     unsigned long ticks=ff_gatherer::TICKS2WAIT) = delete;
    void cleanup_workers()                                        = delete;
    void cleanup_all()                                            = delete;
    bool load_result_nb(void ** task)                             = delete;
};

#endif
