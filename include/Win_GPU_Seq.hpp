#ifndef WIN_GPU_SEQ_HPP
#define WIN_GPU_SEQ_HPP

/*!
 *
 *  This is the sequential pattern of all the window-based stream processing
 *  computations over GPU. It operates on one or more sliding windows depending on the
 *  used context (single keyed or (still experimental)multi keyed streams).
 *
 *  Computation is performed over GPU, by batching a certain number of Windows before starting the computation
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
 *	Author: Tiziano De Matteis <dematteis at di.unipi.it>
 */


#include <vector>
#include <memory>
#include <functional>
#include <ff/node.hpp>
#include "Win_GPU_Manager.hpp"
#include "Win_Allocators.hpp"
#include "Win_GPU_Config.h"

using namespace ff;

/* This file provides the following classes:
 *  Win_GPU_Seq is the class implementing the Sequential Window-based Pattern over GPU. It creates and handles a CUDA stream toward the GPU
 *  Win_Seq_Wrapper is the class that implements a wrapper to the standard Win_Seq (used by the KP and WF patterns)
 */


// Util: get the Streaming Multiprocessor ID on which the kernel is in execution
__device__ uint get_smid(void) {

	 uint ret;

	 asm("mov.u32 %0, %smid;" : "=r"(ret) );

	 return ret;

}

/**
 *	Cuda Kernel: it invokes the function passed by the user over the different windows
 */
template<typename T_IN, typename T_OUT,typename CUDA_F>
__global__ void kernelWindow(T_IN* data, unsigned long *start, unsigned long *end,T_OUT *res, CUDA_F fun, int batch_size)
{
	int id= threadIdx.x + blockIdx.x*blockDim.x;
//	printf("CUDA [%d] launching... from %ld to %ld\n",id,start[id],end[id]);
	if(id<batch_size)
		fun(data+start[id],&res[id],end[id]-start[id]);
//	printf("CUDA [%d] ...finished\n",id);
//	printf("Executed on SMID %d\n",get_smid());
}

/**
 *	Cuda Kernel with scratchpad: it invokes the function passed by the user over the different windows
 */
template<typename T_IN, typename T_OUT,typename CUDA_F>
__global__ void kernelWindowScratchpad(T_IN* data, unsigned long *start, unsigned long *end,T_OUT *res, CUDA_F fun, int batch_size, char *scratchpad_memory, size_t scratchpad_size)
{
	int id= threadIdx.x + blockIdx.x*blockDim.x;
//	printf("CUDA [%d] launching... from %ld to %ld\n",id,start[id],end[id]);
	if(id<batch_size)
	{
		if(scratchpad_size>0)
			fun(data+start[id],&res[id],end[id]-start[id],&scratchpad_memory[id*scratchpad_size]);
		else
			fun(data+start[id],&res[id],end[id]-start[id],nullptr);
	}
}

#define gpuErrchk(ans) { gpuAssert((ans), __FILE__, __LINE__); }
inline void gpuAssert(cudaError_t code, const char *file, int line, bool abort=false)
{
   if (code != cudaSuccess)
   {
	  fprintf(stderr,"GPUassert: %s %s %d\n", cudaGetErrorString(code), file, line);
	  if (abort) exit(code);
   }
}


/*!
 *  \class Win_GPU_Seq
 *  \ingroup high_level_patterns
 *
 *  \brief Sequential Window-based Pattern
 *
 *  The implementation is derived from the Win_Seq.hpp
 *  This is the sequential window-based pattern. For each input tuple it performs
 *  the following actions:
 *  1-identify the key of the current tuple using a user function;
 *  2-insert the tuple in the corresponding window (or create it if it does not exist);
 *  3-check if the window has been triggered and in the case execute a user function.
 *		In this case it copies the data on the GPU, starts the computation and get back
 *		the results. If overlapping is enabled, these last three steps are performed asynchronously
 *
 *  Each Win_GPU_Seq node will use a different cuda stream to address commands to the GPU
 *
 *  NOTE: for the moment being it works only for Count Based Window since it preallocates GPU memory.
 *  To use also Time Based Window this aspect must be changed (the rest of the implementation should work)
 *
 *
 *  An instance of a Win_GPUSeq makes use of two external allocator objects. The first
 *  is an allocator used for the input tuples, the second an allocator that will be
 *  used to allocate the output results by the pattern. Their types are specified as
 *  template arguments.
 *
 *  This class is defined in \ref HLPP_patterns/include/Win_Seq.hpp
 */
template<typename IN_T, typename OUT_T, typename WIN_T, typename CUDA_F_T,typename Ext_Alloc_IN_T=std::allocator<IN_T>, typename Ext_Alloc_OUT_T=std::allocator<OUT_T>>
class Win_Seq: public ff_node_t<IN_T, OUT_T> {
private:
	typedef IN_T in_tuple_t;
	typedef OUT_T out_tuple_t;
	typedef WIN_T win_type_t;
	typedef typename win_type_t::win_container_t win_container_t;
	typedef std::function<std::pair<unsigned int, unsigned long>(const in_tuple_t&)> FUNC_IN_T; // function to retrieve the key id and tuple id from an input tuple
	typedef std::function<void(out_tuple_t*, int , out_tuple_t* )> FUNC_H_T;	//pane aggregation function (for Pane Based version only)

	FUNC_IN_T _M_IN;
	CUDA_F_T _CudaF;
	FUNC_H_T _Fun_H							=0;							//the function used for aggregation in the Pane Based version
	bool _pane_version						=false;
	int _pane_size							=-1;
	int _panes_per_batch					=-1;							//number of panes per batch
	unsigned long _win_length;
	unsigned long _win_slide;
	unsigned int _id						=0; 						//the worker id

	std::vector<std::unique_ptr<win_type_t>> _winVector;				// vector of unique pointers to the window data structures (one per key) used by the Win_Seq
	bool _isWrapped;													// this flag is true if the Win_Seq is used inside a Win_Seq_Wrapper false otherwise
	const unsigned int _resize_factor=3000;								// resize factor used by the internal vectors (winVector)
	Ext_Alloc_IN_T* _ext_alloc_in;										// pointer to an external allocator of input tuples
	Ext_Alloc_OUT_T* _ext_alloc_out;									// pointer to an external allocator of output results
	bool freeAllocIN=false;												// true if the input external allocator have been created into the pattern, false otherwise
	bool freeAllocOUT=false;											// true if the output external allocator have been created into the pattern, false otherwise
	unsigned long _win_batch_size;										//in the case of multiple windows (e.g. GPU_CB_WINDOW)  it indicates the number of windows that have to be batched before computing. To be passed to the window constructor
	unsigned long _max_batch_size;									//the starting batch size
	unsigned long _par_degree;											//number of workers
	ff_node *_parent;													//parent reference, (i.e. the node that encapsulate it) in order to send out results

	//gpu data structures
	IN_T *_gpu_data;													//window data
	OUT_T *_gpu_res, *_host_res;										//results (gpu and host side)
	unsigned long *_gpu_start, *_gpu_end, *_host_start, *_host_end; 	//data ranges (each one identifies the content of the window)
	cudaStream_t _cuda_stream;											//cuda stream to which issue gpu commands
	bool _overlap_accumulation;											//true if the worker can accumulate the next window while the previous window is computing
	bool _pending_work;													// true if there is work on the gpu (meaningful only with _overlap_accumulation=true)
	std::vector<std::pair<unsigned long, unsigned long>> _idxs;
	OUT_T *_host_wpane_res;												//in case of pane based version, this will contain the result of the aggregation on pane results (i.e. the final window results)

	//debug and profiling
	long _comp_time=0;
	long _numb_comp=0;
	long _dequeue_time=0;
	long _start_dequeue_time=0;

	//for communicating with the manager and monitoring the computation
	//NOTE: in this version we will not send from workers to manager. These queues are unused
	SWSR_Ptr_Buffer *_to_manager;
	bool send_to_manager=false; //tmp
	const int _num_monitoring_data=10;	//how many monitoring data we must collect before sending the data to the manager
	std::vector<long> _monitored_rq;


	size_t _scratchpad_size=0;
	char *_gpu_scratchpad_memory;											//GPU memory allocated for kernel computation (a portion for each cuda core)



public:



	/**
	 * \brief constructor for standard case
	 *
	 * \param M_IN the function to extract the key and tuple id values from a tuple
	 * \param CF business code to be executed over the gpu
	 * \param wlen the length of the window
	 * \param wslide the sliding factor of the window
	 * \param ext_alloc_out reference to an external allocator of output results (if not specified the type is assumed of a stateless allocator)
	 * \param window_batch_size: length of the window batch, meaningfull in the case of Multiple_Window
	 * \param p: parent node (the wrapper) used to sendout the results
	 * \param cuda_stream: stream over wich send the GPU commands
	 * \param overlap_accumulation: true if we can overlap GPU computation with data accumulation
	 * \param _resize_factor number of keys initially assumed in the internal vectors (they will be resized by this factor each time, if needed)
	 *
	 */
	Win_Seq(FUNC_IN_T M_IN, CUDA_F_T CF, unsigned long wlen, unsigned long wslide, Ext_Alloc_OUT_T& _ext_alloc_out, unsigned long window_batch_size,
			unsigned long max_batch_size,ff_node *p, unsigned long par_degree, cudaStream_t cuda_stream, bool overlap_accumulation,
			SWSR_Ptr_Buffer *to_manager, size_t scratchpad_size)
			:_M_IN(M_IN), _CudaF(CF), _win_length(wlen), _win_slide(wslide), _winVector(3000), _win_batch_size(window_batch_size),
			  _max_batch_size(max_batch_size),_parent(p), _isWrapped(true),_ext_alloc_out(&_ext_alloc_out),_par_degree(par_degree),
			  _cuda_stream(cuda_stream), _overlap_accumulation(overlap_accumulation), _pending_work(false),
			  _to_manager(to_manager), _scratchpad_size(scratchpad_size)
	{

//		printf("Worker, max bs: %ld\n",_max_batch_size);
		//GPU memory allocation: consider the max batch size
		//for count based window the size (in terms of element) that must be hold to maintain B consecutive windowscan be easily computed as
		int global_w_length=_win_slide*(_max_batch_size-1) + _win_length;

		gpuErrchk(cudaMalloc((unsigned long **) &_gpu_start,_max_batch_size*sizeof(unsigned long)))
		gpuErrchk(cudaMalloc((unsigned long **) &_gpu_end,_max_batch_size*sizeof(unsigned long)));
		gpuErrchk(cudaMalloc((IN_T **)&_gpu_data,global_w_length*sizeof(IN_T)));
		gpuErrchk(cudaMalloc((OUT_T **)&_gpu_res,_max_batch_size*sizeof(OUT_T)));	//at maximum each worker can lauch MAX_BATCH_SIZE

		_host_start=new unsigned long[_max_batch_size]();
		_host_end=new unsigned long[_max_batch_size]();
		gpuErrchk(cudaMallocHost((void**)&_host_res,_max_batch_size*sizeof(OUT_T)));

		//if scratchpad memory is required we have to allocate scratchpad_size byte for each batch
		//PAY ATTENTION: this could be a large number and it could not fit in the GPU memory
		if(_scratchpad_size>0)
		{
			//printf("Alloco %d\n",_max_batch_size*_scratchpad_size);
			gpuErrchk(cudaMalloc((char **)&_gpu_scratchpad_memory,_max_batch_size*_scratchpad_size));	//at maximum each worker can lauch MAX_BATCH_SIZE
		}
		_monitored_rq.reserve(5);
	}


	/**
	 * \brief constructor for Pane Case
	 *
	 * \param M_IN the function to extract the key and tuple id values from a tuple
	 * \param CF business code to be executed over the gpu
	 * \param wlen the length of the window
	 * \param wslide the sliding factor of the window
	 * \param ext_alloc_out reference to an external allocator of output results (if not specified the type is assumed of a stateless allocator)
	 * \param window_batch_size: length of the window batch, meaningfull in the case of Multiple_Window
	 * \param p: parent node (the wrapper) used to sendout the results
	 * \param cuda_stream: stream over wich send the GPU commands
	 * \param overlap_accumulation: true if we can overlap GPU computation with data accumulation
	 * \param _resize_factor number of keys initially assumed in the internal vectors (they will be resized by this factor each time, if needed)
	 *
	 */
	Win_Seq(FUNC_IN_T M_IN, CUDA_F_T CF, FUNC_H_T H, unsigned long wlen, unsigned long wslide, Ext_Alloc_OUT_T& _ext_alloc_out, unsigned long window_batch_size,
			unsigned long max_batch_size,ff_node *p, unsigned long par_degree, cudaStream_t cuda_stream, bool overlap_accumulation,
			SWSR_Ptr_Buffer *to_manager, size_t scratchpad_size)
			:_M_IN(M_IN), _CudaF(CF), _win_length(wlen), _win_slide(wslide), _winVector(3000), _win_batch_size(window_batch_size),
			  _max_batch_size(max_batch_size),_parent(p), _isWrapped(true),_ext_alloc_out(&_ext_alloc_out),_par_degree(par_degree),
			  _cuda_stream(cuda_stream), _overlap_accumulation(overlap_accumulation), _pending_work(false),
			  _to_manager(to_manager), _scratchpad_size(scratchpad_size), _Fun_H(H)
	{int panes_in_batch=(_win_slide*(_idxs.size()-1) + _win_length)/_pane_size;

		int global_w_length=_win_slide*(_max_batch_size-1) + _win_length;
		_pane_version=true;
		_pane_size=gcd(wlen,wslide);
		_panes_per_batch=(_win_slide*(window_batch_size-1) + _win_length)/_pane_size;
		//the GPU will produce as much as pane results as the number of pane in the batch. While allocating
		//we have to consider tha max batch size
		int max_panes_per_batch=global_w_length/_pane_size;
		printf("Pane version, with pane of size: %d. Number of panes per batch: %d\n",_pane_size,_panes_per_batch);

		gpuErrchk(cudaMalloc((unsigned long **) &_gpu_start,max_panes_per_batch*sizeof(unsigned long)))
		gpuErrchk(cudaMalloc((unsigned long **) &_gpu_end,max_panes_per_batch*sizeof(unsigned long)));
		gpuErrchk(cudaMalloc((IN_T **)&_gpu_data,global_w_length*sizeof(IN_T)));
		gpuErrchk(cudaMalloc((OUT_T **)&_gpu_res,max_panes_per_batch*sizeof(OUT_T)));	//at maximum each worker can lauch MAX_BATCH_SIZE

		_host_start=new unsigned long[max_panes_per_batch]();
		_host_end=new unsigned long[max_panes_per_batch]();
		gpuErrchk(cudaMallocHost((void**)&_host_res,max_panes_per_batch*sizeof(OUT_T)));

		//this will contain the final window results
		_host_wpane_res=new OUT_T[_max_batch_size]();
		//if scratchpad memory is required we have to allocate scratchpad_size byte for each batch
		//PAY ATTENTION: this could be a large number and it could not fit in the GPU memory
		if(_scratchpad_size>0)
		{
			//printf("Alloco %d\n",_max_batch_size*_scratchpad_size);
			gpuErrchk(cudaMalloc((char **)&_gpu_scratchpad_memory,max_panes_per_batch*_scratchpad_size));	//at maximum each worker can lauch MAX_BATCH_SIZE
		}
		_monitored_rq.reserve(5);


	}




	// destructor
	~Win_Seq() {
		if(freeAllocIN) delete _ext_alloc_in;
		if(freeAllocOUT) delete _ext_alloc_out;

		//gpu memory freeing
		cudaFree(_gpu_data);
		cudaFree(_gpu_res);
		cudaFree(_gpu_start);
		cudaFree(_gpu_end);

		delete[] _host_start;
		delete[] _host_end;
		if(_scratchpad_size>0)
			cudaFree(_gpu_scratchpad_memory);
		cudaFreeHost(_host_res);
		//destroy the stream
		cudaStreamDestroy(_cuda_stream);
	}

	// svc_init method
	int svc_init() {
		// initialization of the internal vectors
		for(int i = 0; i < _resize_factor; i++) {
			_winVector[i] = nullptr; // the window is unitialized
		}
		return 0;
	}




	/*
	 * Used to set a new pardegree: it is needed to change the actual sliding size of the window assigned
	 * (i.e. slide_worker=slide*pardegree);
	 */
	void change_pardegree(unsigned int key, long int id, int new_par_degree, int batch_size)
	{
		//If we have change in the batch size (e.g. this could be a worker that was sleeping for a certain amount of time)
		//we have to update it properly (this will require to clean up the window). Otherwise we just need to adjust the expiring
		//slide of the window
		if(batch_size!=_win_batch_size)
			change_batchsize(key,id,batch_size,new_par_degree);
		else
		{
			this->_par_degree=new_par_degree;
			if(_winVector[key]!=nullptr)
				_winVector[key]->change_expiring_slide(_win_batch_size*_win_slide*_par_degree);
		}

//		send_to_manager=true;
	}

	/*
	 *	Used to set a new batch size. This will require to reset the window
	        It changes also the pardegree
	 */
  void change_batchsize(unsigned int key, long int id, int new_batch_size, int pardegree)
	{
		//it could be the case tha the par degree has changed
		this->_par_degree=pardegree;
		//allocate the right amount of memory (if the batch size is greater than the max one
		// otherwise we can continue to use the previous)
		if(new_batch_size>_max_batch_size)	//clearly this should never occur
		{
			int global_w_length=_win_slide*(new_batch_size-1) + _win_length;
			gpuErrchk(cudaFree(_gpu_data));
			gpuErrchk(cudaFree(_gpu_res));
			gpuErrchk(cudaFree(_gpu_start));
			gpuErrchk(cudaFree(_gpu_end));

			delete[] _host_start;
			delete[] _host_end;
			gpuErrchk(cudaFreeHost(_host_res));


			gpuErrchk(cudaMalloc((unsigned long **) &_gpu_start,new_batch_size*sizeof(unsigned long)));
			gpuErrchk(cudaMalloc((unsigned long **) &_gpu_end,new_batch_size*sizeof(unsigned long)));
			gpuErrchk(cudaMalloc((IN_T **)&_gpu_data,global_w_length*sizeof(IN_T)));
			gpuErrchk(cudaMalloc((OUT_T **)&_gpu_res,new_batch_size*sizeof(OUT_T)));

			_host_start=new unsigned long[new_batch_size]();
			_host_end=new unsigned long[new_batch_size]();
			gpuErrchk(cudaMallocHost((void**)&_host_res,new_batch_size*sizeof(OUT_T)));
			printf("[%u] We need more memory...allocated\n",this->_id);
			abort();

		}
		this->_win_batch_size=new_batch_size;
		if(_winVector[key]!=nullptr)
		{
			_winVector[key]->change_batch_size(new_batch_size,_par_degree);
		}
		if(_pane_version)
			_panes_per_batch=(_win_slide*(new_batch_size-1) + _win_length)/_pane_size;


	}

	// svc method
	out_tuple_t* svc(in_tuple_t* in) {
		std::pair<unsigned int, unsigned long> in_info = _M_IN(*in);
		unsigned int key = in_info.first; // extract the key

		unsigned long id = in_info.second; // extract the id of the tuple
		// check if the key is in the actual vectors, otherwise resize them
		if (key >= _winVector.size()) {
			unsigned int old_size = _winVector.size();
			// resize the two vectors
			_winVector.resize(key+1);
			// initialize the new elements of the two vectors
			for(int i = old_size; i < key+1; i++) {
				_winVector[i] = nullptr; // the window is unitialized
			}
		}
		// check if the window is initiliazed, otherwise initialize it

		if(_winVector[key] == nullptr) {
			//monitoring (required for the first initialization
			if(_start_dequeue_time==0)
				_start_dequeue_time=ff::getusec();


			std::unique_ptr<win_type_t> w(new win_type_t(_win_length, _win_slide,_win_batch_size,_win_batch_size*_win_slide*_par_degree));
			_winVector[key] = std::move(w);
		}

		// insert a copy of the tuple in the window and check whether it is triggered or not
		bool triggered = _winVector[key]->insert(*in);
		// if Win_Seq is not wrapped we destroy and deallocate the input tuple using the external input allocator
		if(!_isWrapped) {
			_ext_alloc_in->destroy(in);
			_ext_alloc_in->deallocate(in, 1);
		}
		// if not triggered go on
		if(!triggered) {
			return ff_node_t<in_tuple_t, out_tuple_t>::GO_ON;
		}
		// else compute the window
		else {
			//save dequeue time
			long start_usec=ff::getusec();
			_dequeue_time+=ff::getusec()-_start_dequeue_time;


			//if we overlap data accumulation with gpu computation and there is currently pending work we have to wait for its result
			if(_overlap_accumulation && _pending_work)
			{
				gpuErrchk(cudaStreamSynchronize(_cuda_stream));

				//send out results
				for(int i=0;i<_idxs.size();i++)
				{
					out_tuple_t* out = _ext_alloc_out->allocate(1);
					_ext_alloc_out->construct(out);
					*out=_host_res[i];
					while(!_parent->ff_send_out(out));
				}
				_pending_work=false;
			}

			//CUDA SECTION

			// allocate and construct the output data structure using the external output allocator
			//here we call the user functions over the B windows

			//if we are in the standard case (i.e. generic function F computed from scratch)
			//each Cuda Thread will work on a window. If we are in the pane case each
			//thread will work on a different pane (of length pane_size)
			_idxs=(_winVector[key])->get_indexes();



//			int global_w_length=((_winVector[key])->get_content()).size();
			int global_w_length=_win_slide*(_win_batch_size-1) + _win_length;


			//fill kernel support info
			if(!_pane_version)
			{
				for(int i=0;i<_idxs.size();i++)
				{
					_host_start[i]=_idxs[i].first;
					_host_end[i]=_idxs[i].second;
				}

				//copy data to gpu
				gpuErrchk(cudaMemcpyAsync(_gpu_start,_host_start,_win_batch_size*sizeof(unsigned long),cudaMemcpyHostToDevice,_cuda_stream));
				gpuErrchk(cudaMemcpyAsync(_gpu_end,_host_end,_win_batch_size*sizeof(unsigned long),cudaMemcpyHostToDevice,_cuda_stream));
				gpuErrchk(cudaMemcpyAsync(_gpu_data,((_winVector[key])->get_content()).data(),sizeof(IN_T)*global_w_length,cudaMemcpyHostToDevice,_cuda_stream));

			}
			else
			{
				//TODO: versione PANE per il  momento creiamo noi gli indici ma attenzione, va generalizzato
				//si parte da 0 e poi si aggiunge la pane_size, fino a coprire panes_per_batch panes
				unsigned long idx=0;

				for(int i=0;i<_panes_per_batch;i++)
				{
					_host_start[i]=idx;
					_host_end[i]=idx+_pane_size;
					idx+=_pane_size;
				}

				//copy data to gpu
				gpuErrchk(cudaMemcpyAsync(_gpu_start,_host_start,_panes_per_batch*sizeof(unsigned long),cudaMemcpyHostToDevice,_cuda_stream));
				gpuErrchk(cudaMemcpyAsync(_gpu_end,_host_end,_panes_per_batch*sizeof(unsigned long),cudaMemcpyHostToDevice,_cuda_stream));
				gpuErrchk(cudaMemcpyAsync(_gpu_data,((_winVector[key])->get_content()).data(),sizeof(IN_T)*global_w_length,cudaMemcpyHostToDevice,_cuda_stream));

			}


			//launch kernel definition: compute the number of blocks needed according to MAX_CUDA_THREADS_PER_BLOCK
			int num_blocks;
			if(!_pane_version)
			{
				num_blocks=ceil((double)_win_batch_size/MAX_CUDA_THREADS_PER_BLOCK);
				//always call the version with the scratchpad (in case it is defined as nullptr)
				//gpuErrchk(cudaStreamSynchronize(_cuda_stream));
				//long start_usec=ff::getusec();
				kernelWindowScratchpad<IN_T,OUT_T><<<num_blocks,MAX_CUDA_THREADS_PER_BLOCK,0,_cuda_stream>>>(_gpu_data,_gpu_start,_gpu_end,_gpu_res,_CudaF,_win_batch_size,_gpu_scratchpad_memory,_scratchpad_size);
				//gpuErrchk(cudaStreamSynchronize(_cuda_stream));
				//_comp_time+=ff::getusec()-start_usec;

			}
			else //the number of pane in the batch is given by global_w_lenth/pane_size
			{

				num_blocks=ceil((double)(global_w_length/_pane_size)/MAX_CUDA_THREADS_PER_BLOCK);
				kernelWindowScratchpad<IN_T,OUT_T><<<num_blocks,MAX_CUDA_THREADS_PER_BLOCK,0,_cuda_stream>>>(_gpu_data,_gpu_start,_gpu_end,_gpu_res,_CudaF,(global_w_length/_pane_size),_gpu_scratchpad_memory,_scratchpad_size);

			}


//			kernelWindow<IN_T,OUT_T><<<num_blocks,MAX_CUDA_THREADS_PER_BLOCK,0,_cuda_stream>>>(_gpu_data,_gpu_start,_gpu_end,_gpu_res,_CudaF,_win_batch_size);

			_pending_work=true;

			//copy result back
			if(!_pane_version)
			{
				gpuErrchk(cudaMemcpyAsync(_host_res,_gpu_res,_win_batch_size*sizeof(OUT_T),cudaMemcpyDeviceToHost,_cuda_stream));
			}
			else
			{
				gpuErrchk(cudaMemcpyAsync(_host_res,_gpu_res,(global_w_length/_pane_size)*sizeof(OUT_T),cudaMemcpyDeviceToHost,_cuda_stream));

			}


			if(!_overlap_accumulation) //wait for the result
			{
				gpuErrchk(cudaStreamSynchronize(_cuda_stream));
				if(_pane_version)
				{
					//Now we have to perform the H evaluation over the pane results
					//Each window is comprised of win_length/pane_size panes
					//Between windows we advace of slide/pane_size panes
					int pane_per_window=_win_length/_pane_size;
					int pane_per_slide=_win_slide/_pane_size;
					OUT_T* pane_results=_host_res;
//					for(int i=0;i<_panes_per_batch;i++)
//						printf("%d: %Ld\n",i,pane_results[i].timestamp);
					for(int i=0; i<_idxs.size();i++)	//TODO: usare altro rispetto a questo
					{
						_Fun_H(pane_results,pane_per_window,&(_host_wpane_res[i]));
//						printf("Copiati: %d - id: %d - Timestamp: %d\n",(global_w_length/_pane_size),pane_results[0].id,pane_results[pane_per_window-1].timestamp);
						pane_results+=pane_per_slide;
					}

					//TODO: gestire anche i casi in svc_end
				}


				//send out results
				for(int i=0;i<_idxs.size();i++)
				{
	//				TODO: da sistemare questa faccenda dell'allocazione. Questa copia puo' essere pesante
	//				ma non riesco a fare altrimenti senza farlo schiantare
					out_tuple_t* out = nullptr;
					_ext_alloc_out->allocate(out,1);
					if(!_pane_version)
						*out=_host_res[i];
					else
						*out=_host_wpane_res[i];
					while(!_parent->ff_send_out(out));

				}


			}

			//get metrics
			long rq=ff::getusec()-start_usec;
			_comp_time+=rq;
			_numb_comp++;
			_start_dequeue_time=ff::getusec();

			//this always return GO_ON
			 return ff_node_t<in_tuple_t, out_tuple_t>::GO_ON;
		}
	}

	void svc_end()
	{
		//When a worker goes to sleep we have to clear all its window {otherwise at wake up they are not valid)
		for(int k=0;k<_winVector.size();k++)
		{
			if(_winVector[k])
				_winVector[k]->reset();
		}

	}

    
	// eos notify method
	void eosnotify(ssize_t id=-1)
	{


		if(_overlap_accumulation && _pending_work)
		{
			//if overlap is enable get results from previous case
			cudaStreamSynchronize(_cuda_stream);
			//TODO sistemare versione PANE
			//send out results
			for(int i=0;i<_idxs.size();i++)
			{
//				TODO: da sistemare questa faccenda dell'allocazione. Questa copia puo' essere pesante
//				ma non riesco a fare altrimenti senza farlo schiantare
				out_tuple_t* out = nullptr;
				_ext_alloc_out->allocate(out,1);
				_ext_alloc_out->construct(out);
				*out=_host_res[i];
				while(!_parent->ff_send_out(out));

			}
		}
		//we have to send all the windows that we have accumulated so far for the different keys
		for(int k=0;k<_winVector.size();k++)
		{
			if(_winVector[k])
			{
				//get the windows
				//note: we compute also the calculation time
				long start_usec=ff::getusec();
				//here we call the user functions over the B windows
				_idxs=(_winVector[k])->get_partial_indexes();

				//CUDA SECTION-

				//allocate the space for the data

				int num_of_windows=_idxs.size();
				int panes_in_batch=(_win_slide*(_idxs.size()-1) + _win_length)/_pane_size;	//valid for pane version only

				//if the number of windows (currently maintained) is exactly equal to the window batch
				//this means that they were already computed and we can skip this
				if(num_of_windows==_win_batch_size)
					continue;

				int w_length=((_winVector[k])->get_content()).size();


				//fill kernel support info
				if(!_pane_version)
				{
					for(int i=0;i<_idxs.size();i++)
					{
						_host_start[i]=_idxs[i].first;
						_host_end[i]=_idxs[i].second;
					}
					//copy data to gpu
					gpuErrchk(cudaMemcpyAsync(_gpu_start,_host_start,num_of_windows*sizeof(unsigned long),cudaMemcpyHostToDevice,_cuda_stream));
					gpuErrchk(cudaMemcpyAsync(_gpu_end,_host_end,num_of_windows*sizeof(unsigned long),cudaMemcpyHostToDevice,_cuda_stream));
					gpuErrchk(cudaMemcpyAsync(_gpu_data,((_winVector[k])->get_content()).data(),sizeof(IN_T)*w_length,cudaMemcpyHostToDevice,_cuda_stream));
				}
				else
				{
					unsigned long idx=0;
					//_idxs.size is the number of valid window in the batch
					//in total we will have a certain number of panes
					panes_in_batch=(_win_slide*(_idxs.size()-1) + _win_length)/_pane_size;
					for(int i=0;i<panes_in_batch;i++)
					{
						_host_start[i]=idx;
						_host_end[i]=idx+_pane_size;
						idx+=_pane_size;
					}
					gpuErrchk(cudaMemcpyAsync(_gpu_start,_host_start,panes_in_batch*sizeof(unsigned long),cudaMemcpyHostToDevice,_cuda_stream));
					gpuErrchk(cudaMemcpyAsync(_gpu_end,_host_end,panes_in_batch*sizeof(unsigned long),cudaMemcpyHostToDevice,_cuda_stream));
					gpuErrchk(cudaMemcpyAsync(_gpu_data,((_winVector[k])->get_content()).data(),sizeof(IN_T)*w_length,cudaMemcpyHostToDevice,_cuda_stream));

				}


				//launch kernel definition:
				//this depends on the number of windows that are valid
				int num_blocks;
				if(!_pane_version)
				{
					num_blocks=ceil((double)num_of_windows/MAX_CUDA_THREADS_PER_BLOCK);
					kernelWindowScratchpad<IN_T,OUT_T><<<num_blocks,MAX_CUDA_THREADS_PER_BLOCK,0,_cuda_stream>>>(_gpu_data,_gpu_start,_gpu_end,_gpu_res,_CudaF,num_of_windows,_gpu_scratchpad_memory,_scratchpad_size);
				}
				else
				{
					num_blocks=ceil((double)panes_in_batch/MAX_CUDA_THREADS_PER_BLOCK);
					kernelWindowScratchpad<IN_T,OUT_T><<<num_blocks,MAX_CUDA_THREADS_PER_BLOCK,0,_cuda_stream>>>(_gpu_data,_gpu_start,_gpu_end,_gpu_res,_CudaF,panes_in_batch,_gpu_scratchpad_memory,_scratchpad_size);
				}



				_pending_work=true;


				//copy result back
				if(!_pane_version)
				{
					gpuErrchk(cudaMemcpyAsync(_host_res,_gpu_res,num_of_windows*sizeof(OUT_T),cudaMemcpyDeviceToHost,_cuda_stream));
				}
				else
				{
					gpuErrchk(cudaMemcpyAsync(_host_res,_gpu_res,panes_in_batch*sizeof(OUT_T),cudaMemcpyDeviceToHost,_cuda_stream));

				}


				gpuErrchk(cudaStreamSynchronize(_cuda_stream));

				if(_pane_version)
				{
					//Now we have to perform the H evaluation over the pane results
					//Each window is comprised of win_length/pane_size panes
					//Between windows we advace of slide/pane_size panes
					int pane_per_window=_win_length/_pane_size;
					int pane_per_slide=_win_slide/_pane_size;
					OUT_T* pane_results=_host_res;
					for(int i=0; i<_idxs.size();i++)	//TODo: usare altro rispetto a questo
					{
						_Fun_H(pane_results,pane_per_window,&_host_wpane_res[i]);
						pane_results+=pane_per_slide;
					}

					//TODO: gestire anche i casi in svc_end
				}


				//printf("%ud sending out %d results\n",this->_id,num_of_windows);
				//send out results
				for(int i=0;i<num_of_windows;i++)
				{
					out_tuple_t* out = _ext_alloc_out->allocate(1);
					_ext_alloc_out->construct(out);
					if(!_pane_version)
						*out=_host_res[i];
					else
						*out=_host_wpane_res[i];
					while(!_parent->ff_send_out(out));

				}


				_comp_time+=ff::getusec()-start_usec;

				_numb_comp++;

			}


		}
		if(_numb_comp>0)
			printf("[%u] Number of computation: %ld avg. comp. time: %.3f\n",_id,_numb_comp,((double)_comp_time)/_numb_comp);
//		printf("Avg. dequeue time: %.3f\n",((double)_dequeue_time)/_numb_comp);

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
	//NOTE: no more used: prone do be deleted
	virtual void enable_batch(unsigned long length) {
	}


	// method to disable batching
	//NOTE:enable_batcd: prone do be deleted
	virtual void disable_batch() {
	}

	//set worker id
	void set_id(int id)
	{
		_id=id;
	}
private:

	int gcd(int a, int b)
	{
		for (;;)
		{
			if (a == 0) return b;
			b %= a;
			if (b == 0) return a;
			a %= b;
		}
	}
};

/*!
 *  \class Win_Seq_Wrapper
 *  \ingroup high_level_patterns
 *
 *  \brief A Wrapper to the Standard Win_GPU_Seq
 *
 *  This is a wrapper to the standard Win_Seq. The idea is that each input tuple is wrapped into
 *  a wrapper struct. The same for each output result. This is needed by the Win_KP and the Win_WF patterns.
 *
 *  This class is defined in \ref HLPP_patterns/include/Win_Seq.hpp
 *
 *  In order to pass a CUDA lambda we have to 'template' its type
 */
template<typename IN_T, typename OUT_T, typename WIN_T, typename CUDA_F_T>
class Win_Seq_Wrapper: public ff_node_t<wrapper<IN_T>, wrapper<OUT_T>> {
private:
	typedef IN_T  in_tuple_t;
	typedef OUT_T out_tuple_t;
	typedef WIN_T win_type_t;
	typedef struct wrapper<in_tuple_t> tuple_wrapper_t;
	typedef struct wrapper<out_tuple_t> output_wrapper_t;
	typedef typename win_type_t::win_container_t win_container_t;
	typedef std::function<std::pair<unsigned int, unsigned long>(const in_tuple_t&)> FUNC_IN_T; // function to retrieve the key id and tuple id from an input tuple
	Win_Allocator<out_tuple_t>* _int_alloc_out; // pointer to the Win_Allocator of output results
	typedef std::function<void(out_tuple_t*, int , out_tuple_t* )> FUNC_H_T;	//pane aggregation function (for Pane Based version only)
	Win_Seq<in_tuple_t, out_tuple_t, win_type_t, CUDA_F_T, Win_Allocator<in_tuple_t>, Win_Allocator<out_tuple_t>> myworker;
	unsigned long _window_batch_size;


public:
/**
	 * \brief constructor
	 *
	 * \param _M_IN the function to extract the key and tuple id values from a tuple
	 * \param CF the processing function of the Workers over the GPU
	 * \param _wlen the length of the windows
	 * \param _wslide the sliding factor of the windows
	 * \param _int_alloc_out pointer to the Win_Allocator of output results
	 * \param par_degree how many worker exists in the WF pattern: it is necessary for the windows in order to manage tuples correctly
	 *
	 *
	 */
	Win_Seq_Wrapper(FUNC_IN_T M_IN,  CUDA_F_T CF, unsigned long wlen, unsigned long wslide, cudaStream_t cuda_stream,
					unsigned long window_batch_size, unsigned long max_batch_size,unsigned long par_degree=1, bool overlap_accumulation=false,
					Win_Allocator<out_tuple_t>* int_alloc_out=new Win_Allocator<out_tuple_t>(MAGIC_CONSTANT_OUT),SWSR_Ptr_Buffer *to_manager=nullptr, size_t scratchpad_size=0)
			:myworker(M_IN, CF, wlen, wslide, *(int_alloc_out),window_batch_size,max_batch_size,this, par_degree,cuda_stream,
					  overlap_accumulation,to_manager, scratchpad_size), _int_alloc_out(int_alloc_out), _window_batch_size(window_batch_size){

	}

	/**
		 * \brief constructor II - pane based version
		 *
		 * \param _M_IN the function to extract the key and tuple id values from a tuple
		 * \param CF the processing function of the Workers over the GPU
		 * \param HF the pane aggregation funciton
		 * \param _wlen the length of the windows
		 * \param _wslide the sliding factor of the windows
		 * \param _int_alloc_out pointer to the Win_Allocator of output results
		 * \param par_degree how many worker exists in the WF pattern: it is necessary for the windows in order to manage tuples correctly
		 *
		 *
		 */
		Win_Seq_Wrapper(FUNC_IN_T M_IN,  CUDA_F_T CF, FUNC_H_T HF, unsigned long wlen, unsigned long wslide, cudaStream_t cuda_stream,
						unsigned long window_batch_size, unsigned long max_batch_size,unsigned long par_degree=1, bool overlap_accumulation=false,
						Win_Allocator<out_tuple_t>* int_alloc_out=new Win_Allocator<out_tuple_t>(MAGIC_CONSTANT_OUT),SWSR_Ptr_Buffer *to_manager=nullptr, size_t scratchpad_size=0)
				:myworker(M_IN, CF, HF,wlen, wslide, *(int_alloc_out),window_batch_size,max_batch_size,this, par_degree,cuda_stream,
						  overlap_accumulation,to_manager, scratchpad_size), _int_alloc_out(int_alloc_out), _window_batch_size(window_batch_size){

		}





	// destructor
	~Win_Seq_Wrapper() {
	//	delete _int_alloc_out;

	}


	int svc_init(){
		//just for debug: set worker id
		//printf("Worker %d starting\n",this->get_my_id());

		myworker.set_id(this->get_my_id());
		return 0;

	}


	/**
	 * @brief ff_send_out override of ff_send_out. This is necessary for the wrapper node, to push out (possibly multiple) results
	 * @param task
	 * @param retry
	 * @param ticks
	 * @return true
	 */

	bool ff_send_out(void * task,  unsigned long retry=((unsigned long)-1),	 unsigned long ticks=(ff_node::TICKS2WAIT))
	{

		// allocate and construct the wrapper
		output_wrapper_t* wrapper_out = nullptr;
		_int_alloc_out->allocate(wrapper_out,1);
		_int_alloc_out->construct(wrapper_out);
		// set the wrapper parameters
		wrapper_out->counter = 1; // not meaningful
		wrapper_out->item = (out_tuple_t*)task;
		wrapper_out->allocator = _int_alloc_out;

		while(!ff_node_t<tuple_wrapper_t, output_wrapper_t>::ff_send_out(wrapper_out));

		return true;
	}



	// svc method
	output_wrapper_t* svc(tuple_wrapper_t* wrapper_in) {

#if defined(HARD_RECONFIGURATIONS) || defined(SOFT_RECONFIGURATIONS)
	    if(wrapper_in->tag==PunctuationTag::NONE)
	    {
#endif
		//standard tuple
		in_tuple_t* tuple = wrapper_in->item;
		// compute the possible result by invoking the svc the real worker
		myworker.svc(tuple);
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


		return ff_node_t<tuple_wrapper_t, output_wrapper_t>::GO_ON;

#if defined(HARD_RECONFIGURATIONS) || defined(SOFT_RECONFIGURATIONS)
	    }
	    else
	    {

		//If we are using reconfigurations, we have to clearly differentiate between Hard and Soft reconfigurations.
		//In the latter case we have also to see if we are changing the pardegree only or also the batch size
		//(since the required operations are different)

#if defined(SOFT_RECONFIGURATIONS)
		if(wrapper_in->tag==PunctuationTag::CHANGE_PAR_DEGREE)
		{
		    //set the new pardegree
		    //				printf("Worker %d (%d) ricevuta punctuation da tupla %Ld. Nuovo par degree: %d\n",this->get_my_id(),pthread_self(),wrapper_in->tuple_id,wrapper_in->num_worker);
		    myworker.change_pardegree(wrapper_in->key,wrapper_in->tuple_id, wrapper_in->num_worker,wrapper_in->batch_size);

		}
		else
		    if(wrapper_in->tag==PunctuationTag::CHANGE_BATCH_SIZE)
		    {

//			printf("Worker %d, ricevuto cambio batch size\n",this->get_my_id());
			myworker.change_batchsize(wrapper_in->key,wrapper_in->tuple_id, wrapper_in->batch_size,wrapper_in->num_worker);

		    }
#else
		 if(wrapper_in->tag==PunctuationTag::CHANGE_CONF)
		     myworker.change_batchsize(wrapper_in->key,wrapper_in->tuple_id, wrapper_in->batch_size,wrapper_in->num_worker);
#endif



		//if I'm the first worker, notify to the collector that a reconfiguration has been triggered
		if(this->get_my_id()==0)
		{

//		    printf("Worker %d, mando puncutation al collettore\n",this->get_my_id());

		    output_wrapper_t* punctuation =nullptr;
		    _int_alloc_out->allocate(punctuation,1);
		    _int_alloc_out->construct(punctuation);
		    punctuation->allocator = _int_alloc_out;
		    punctuation->batch_size=wrapper_in->batch_size;
		    punctuation->num_worker=wrapper_in->num_worker; //we have to communicate it in case it has been changed
		    punctuation->tuple_id=wrapper_in->tuple_id; //meaningless
		    punctuation->key=wrapper_in->key;
		    punctuation->tag=wrapper_in->tag;
//		    if(punctuation->tag==PunctuationTag::CHANGE_BATCH_SIZE)
//			printf("Punctutation Batch size\n");
//		    if(punctuation->tag==PunctuationTag::CHANGE_PAR_DEGREE)
//			printf("Punctutation Par degree\n");
		    while(!ff_node_t<tuple_wrapper_t, output_wrapper_t>::ff_send_out(punctuation));
//		    return punctuation;

		}
		Win_Allocator<in_tuple_t>* allocator = wrapper_in->allocator;
		allocator->destroy(wrapper_in);
		allocator->deallocate_all(wrapper_in, 1);
		return ff_node_t<tuple_wrapper_t, output_wrapper_t>::GO_ON;
	    }
#endif

	}


	 void eosnotify(ssize_t id=-1)
	 {
	    //call the worker eosnotify
		 myworker.eosnotify();
	 }

	// svc_end method
	void svc_end() {
//		printf("%d Terminating\n",this->get_my_id());
		myworker.svc_end();
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


#endif // WIN_GPU_SEQ_HPP
