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
 *	Authors: Tiziano De Matteis <dematteis at di.unipi.it>
 *				Gabriele Mencagli <mencagli at di.unipi.it>
 */



#ifndef WIN_ALLOCATOR_H
#define WIN_ALLOCATOR_H
/* This file provides the following classes:
 *  Win_Allocator is a wrapper to the fastflow (stateful) allocator (ff_allocator)
 *  Fake_Allocator is a wrapper to a fake allocator that statically allocates memory space and reuses it
 *  PunctuationTag, used to indicate the type of tuple sent to workers
 */

#include <sys/mman.h>
#include <ff/allocator.hpp>


#define MAGIC_CONSTANT_IN 20000
#define MAGIC_CONSTANT_OUT 2000

using namespace ff;



/*
 */
enum class PunctuationTag{
	NONE,						//standard tuple
	CHANGE_PAR_DEGREE,			//we have to change the par degree
	CHANGE_BATCH_SIZE,			//we have to change the batch size
	CHANGE_CONF					//we have a generic change in the configuration (e.g.  both of them)
};


template<typename T1, typename T2> class Win_Allocator;

// wrapper struct of an input tuple/output result (used by the KP and the WF patterns)
template<typename type_t>
struct wrapper {
	type_t* item;
	Win_Allocator<type_t, wrapper>* allocator;
	std::atomic<int> counter;

	//By means of the Punctuation Tag we indicate if this wrapper contains additional messages for the worker
	PunctuationTag tag=PunctuationTag::NONE;	//by default it is NONE
	int num_worker;								//valid if the tag==CHANGE_PAR_DEGREE, indicates the new number of workers
	long int tuple_id;								// the tuple that generates the punctuation
	unsigned int key;							//NOTE: this have to be fixed. For the moment we use unsigned int since (by pattern definition) this is the type of the key. Not sure this is sufficiently general
	int batch_size;								//valid if tag==CHANGE_BATCH_SIZE or CHANGE_PARD_AND_BSIZE

	wrapper() {}
	wrapper(const struct wrapper<type_t>& w): counter(w.counter.load()), item(w.item), allocator(w.allocator), tag(w.tag), num_worker(w.num_worker)/*, win_id(w.win_id)*/ {}
	struct wrapper<type_t>& operator=(const struct wrapper<type_t>& w) {
		this->counter = w.counter.load();
		this->item = w.item;
		this->allocator = w.allocator;
		this->tag=w.tag;
		this->num_worker=w.num_worker;
		//this->win_id=w.win_id;
	}
};


// base allocator (standard)
template <typename T1, typename T2=wrapper<T1> >
class Win_Allocator {
public:
    Win_Allocator(size_t magic=0) {}
    virtual ~Win_Allocator() {}

    virtual void allocate(T1 *&p, size_t n) { 
        p = (T1*)malloc(n*sizeof(T1)); 
    }
    virtual void allocate(T2 *&p, size_t n) { 
        p = (T2*)malloc(n*sizeof(T2)); 
    }
    
    virtual T1* allocate(size_t n) {
        return (T1*)malloc(n*sizeof(T1));
    }

    virtual void allocate_all(void *&p, size_t n) {
        p = malloc(n * (sizeof(T1)+sizeof(T2))); 
    }
    
    virtual void construct(T1 *p) { new (p) T1(); }
    virtual void construct(T2 *p) { new (p) T2(); }
    
    virtual void construct(T1 *p, const T1 &val) { new (p) T1(val); }
    virtual void construct(T2 *p, const T2 &val) { new (p) T2(val); }

    void destroy(T1* p) {
        p->~T1(); 
    }
    void destroy(T2* p) {
        p->~T2();
    }

    virtual void deallocate(T1 *p, size_t) { free(p); }
    virtual void deallocate(T2 *p, size_t) { free(p); }
    virtual void deallocate_all(void *p, size_t) { free(p); }

    virtual void printstats() {}
};

template <typename T1, typename T2=wrapper<T1> >
class Win_AllocatorFF: public Win_Allocator<T1,T2> {
public:
    Win_AllocatorFF(size_t magic=1000) {

        void *p;
		int r=posix_memalign(&p, 64, sizeof(ff_allocator));
		if(r<0)
			fprintf(stderr,"Error in allocating\n");
        allocator = new (p) ff_allocator;



        const size_t size = sizeof(T1) + sizeof(T2);
        const int slab    = allocator->getslabs(size);
        const int slab2   = -1; //allocator.getslabs(sizeof(T1));
        const int slab3   = -1; //allocator.getslabs(sizeof(T2));
        int nslabs[N_SLABBUFFER];
        for(int i=0;i<N_SLABBUFFER;++i) nslabs[i]=0;
        if(slab<0 ) { //|| slab2<0 || slab3<0) {
            abort();
        }
        else {
            for(int i=0; i<N_SLABBUFFER; i++) {
                if (i==slab || i==slab2 || i==slab3) { nslabs[i]+=magic; }
                else nslabs[i]=0;
            }
            if (allocator->init(nslabs)<0) abort();
        }       	
    }
    
    void allocate(T1 *&p, size_t n) { 
        //if(!isRegistered) {
        //allocator.registerAllocator();
        //isRegistered = true;
        //}

        //p = (T1*)allocator.malloc(n*sizeof(T1)); 
        allocate_all((void*&)p, n);


    }
    void allocate(T2 *&p, size_t n) { 
        //if(!isRegistered) {
        //allocator.registerAllocator();
        //isRegistered = true;
        //}
        //p = (T2*)allocator.malloc(n*sizeof(T2)); 
        allocate_all((void*&)p, n);
    }
    
    void allocate_all(void *&p, size_t n) {
        if(!isRegistered) {
            allocator->registerAllocator();
            isRegistered = true;
        }
        p = allocator->malloc(n * (sizeof(T1)+sizeof(T2))); 
    }
    
    void deallocate(T1 *p, size_t) { allocator->free(p); }
    void deallocate(T2 *p, size_t) { allocator->free(p); }
    void deallocate_all(void *p, size_t) { allocator->free(p); }

    void printstats() {
#if defined(ALLOCATOR_STATS)
        allocator->printstats(std::cout);
#endif
    }

private:
    ff_allocator *allocator = nullptr;
    bool isRegistered = false;
};


template <typename T1, typename T2=wrapper<T1> >
class Win_StaticAllocator: public Win_Allocator<T1,T2> {
    struct type_t {
        size_t idx;
        T2 a;
        T1 b;
        //size_t FF_MEM_ALIGN(idx,64);
    };
public:
	Win_StaticAllocator(const size_t size):size(size),ready(size) {
//		ready.reserve(size); //This create problems in compilation phase: added statically reservation
        cnt.store(0);
        void* result = 0;
        result = mmap(NULL, size*sizeof(type_t), PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
        if (result == MAP_FAILED) abort();
        segment = (type_t*)result;
        for(size_t i=0;i<size;++i) ready[i] = true;

        printf("allocated %ld bytes\n", size*sizeof(type_t));

    }
    ~Win_StaticAllocator() {
	if (munmap(segment, size*sizeof(type_t)) != 0) {
	    printf("Error unmapping memory region\n");
	}
    }

#if 0
    void allocate_all(void *&p, size_t n) {
        // wait for ready data
    L1:
        size_t modulo = cnt % size;
        while(!ready[modulo].load(std::memory_order_acquire)) {
            modulo = cnt++ % size;
        }
        bool oldready = true;
        if (!ready[modulo].compare_exchange_weak(oldready, false, 
						 std::memory_order_release,
						 std::memory_order_relaxed)) {
            goto L1;
        }	
        segment[modulo].idx = modulo;
        p = (void*)&(segment[modulo].a);
        assert(ready[modulo] == false);
    }
#endif
    void allocate_all(void *&p, size_t n) {
        // wait for ready data

    L1:
        int k=0;
        size_t modulo = cnt.fetch_add(1, std::memory_order_relaxed)  % size;
        //size_t modulo_start = modulo;
        for(;!ready[modulo].load(std::memory_order_acquire); 
            modulo = cnt.fetch_add(1, std::memory_order_relaxed)  % size, k++);
        
        //if (k>100) printf("%d - %ld, %ld\n", k, modulo_start, modulo);

        bool oldready = true;
        if (!ready[modulo].compare_exchange_weak(oldready, false, 
						 std::memory_order_release,
						 std::memory_order_relaxed)) {
            goto L1;
        }	
        segment[modulo].idx = modulo;
        p = (void*)&(segment[modulo].a);
        assert(ready[modulo] == false);
    }

    
    void deallocate_all(void *p, size_t) { 
        type_t *p2 = (type_t*)((char*)p - sizeof(size_t));
        assert(ready[p2->idx].load(std::memory_order_acquire) == false);
        ready[p2->idx].store(true, std::memory_order_release); 

        //if (cnt.load(std::memory_order_relaxed) > size) cnt.store(0, std::memory_order_relaxed);
    }

    T1* allocate(size_t n) {
        void *p2;
        allocate_all(p2, n); 
        return (T1*)((char*)p2 + sizeof(T2));
    }

    void allocate(T1 *&p, size_t n) { 
        void *p2;
        allocate_all(p2, n);        
        p = (T1*)((char*)p2 + sizeof(T2));
    }
    void deallocate(T1 *p, size_t) { 
        T2 *p2 = (T2 *)((char *)p - sizeof(T2));
        deallocate_all((void*)p2, 1); 
    }
    void allocate(T2 *&p, size_t n) { 
        allocate_all((void*&)p, n);
    }
    void deallocate(T2 *p, size_t) { 
        deallocate_all((void*)p,1); 
    }

    void printstats() { 
    }

private:
    std::atomic_ullong FF_MEM_ALIGN(cnt, 64);
    type_t* FF_MEM_ALIGN(segment,64); 

    const size_t size;
    std::vector<std::atomic_bool> ready;
};


#endif  // WIN_ALLOCATOR
