/**
  Testing a templated kernel
*/

#ifndef TEST_TEMPLATE_CU
#define TEST_TEMPLATE_CU
#include <iostream>
#include <vector>
#include <functional>
using namespace std;

// struct of the input data type
typedef struct input_struct {
	int key;
	int id;
	long int value;
} input_t;

// struct of the output data type
typedef struct output_struct {
	int key;
	int id;
	long int value;
} output_t;

//struct of the support data structure to correctly invoke the function passed by the user

template<typename T_IN, typename T_OUT>
struct kernel_support_t{
	T_IN *data;	//pointer to the beginning of data
	T_OUT *res; //pointer to result;
	unsigned long size; //length of data
};

__global__ void ff()
{
	printf("Hello %d\n",3);
}

template<typename T_IN, typename T_OUT, typename T_F>
__global__ void kernel(kernel_support_t<T_IN,T_OUT> *ks, T_F fun)
{
	//tmp cast
	//input_t* d=static_cast<input_t *>(data);
	//output_t* r=static_cast<output_t*>(res);
	//r->value=0;
	//for(int i=0;i<size;i++)
	//	r->value+=d[i].value;


	fun(ks->data,ks->res,ks->size);
}

template<typename T, typename CUDA_F>
void launch_kernel(vector<T> v, int size, CUDA_F lambda)
{
	//allocate data for the gpu

	input_t *gpu_data;
	output_t *gpu_res, *host_res;
	kernel_support_t<input_t,output_t> *kt_gpu,*kt;

	cudaMalloc((kernel_support_t<input_t,output_t> **)&kt_gpu,sizeof(kernel_support_t<input_t,output_t>));
	cudaMalloc((input_t **)&gpu_data,size*sizeof(input_t));
	cudaMalloc((output_t **)&gpu_res,sizeof(output_t));
	host_res=(output_t*)malloc(sizeof(host_res));
	kt=(kernel_support_t<input_t,output_t>*)malloc(sizeof(kernel_support_t<input_t,output_t>));

	//fill kernel support info
	kt->data=gpu_data;
	kt->res=gpu_res;
	kt->size=size;

	//copy data to gpu
	cudaMemcpy(kt_gpu,kt,sizeof(kernel_support_t<input_t,output_t>),cudaMemcpyHostToDevice);
	cudaMemcpy(gpu_data,v.data(),sizeof(input_t)*size,cudaMemcpyHostToDevice);

	//kernel definition

	kernel<input_t,output_t><<<1,1>>>(kt_gpu,lambda);


	//copy result back
	cudaMemcpy(host_res,gpu_res,sizeof(output_t),cudaMemcpyDeviceToHost);


	cout << "Result "<<host_res->value<< ", expected "<< (size*(size-1))/2<<endl;
	//cudaDeviceSynchronize();

	cudaFree(gpu_data);
	cudaFree(gpu_res);
	cudaFree(kt_gpu);

}

int main(int argc, char *argv[])
{
	const int size = 500;
	//allocate data....this will be the window contet
	vector<input_t> v;
	input_t tmp;
	tmp.key=1;
	for(int i=0;i<size;i++)
	{
		tmp.id=i;
		tmp.value=i;
		v.push_back(tmp);
	}


	auto lambda = [=] __device__ (input_t *d, output_t* r, int size) {
		r->value=0;
		for(int i=0;i<size;i++)
			r->value+=d[i].value;
	  };


	launch_kernel<input_t>(v,size,lambda);

}


#endif // TEST_TEMPLATE_CU
