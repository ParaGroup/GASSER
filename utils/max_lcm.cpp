/**
 * Find the maximum lcm in a range of numbers
 */

#include <iostream>
#include <vector>

using namespace std;
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

int lcm(int a, int b)
{
	int temp = gcd(a, b);
	return temp ? (a / temp * b) : 0;
}

int main()
{
	//vector<int> a {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
	//vector<int> b {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
	vector<int> a {384,768,1152,1536,1920,2304,2688,3072,3456,3840,4224,4608,4992,5376,5760,6144};
	vector<int> b {384,768,1152,1536,1920,2304,2688,3072,3456,3840,4224,4608,4992,5376,5760,6144};

	int max_lcm=1;
	int max_a;
	int max_b;

	for(int aa:a)
	{
		for (int bb:b)
			if(lcm(aa,bb)>max_lcm)
			{
				max_lcm=lcm(aa,bb);
				max_a=aa;
				max_b=bb;
			}
	}

	cout << "Maximum lcm: "<< max_lcm<< " obtained with ("<<max_a<<", "<<max_b<<")"<<endl;

}
