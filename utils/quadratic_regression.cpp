/**
 * Example of quadratic regression. Given a vector x and a vector of corresponding y
 * we want to find a,b,c such that
 *				y= ax^2 +bx +c
 * represent the best fitting of that points
 *
 * Source: https://www.easycalculation.com/statistics/learn-quadratic-regression.php
 */

#include <iostream>

int main(int argc,char *argv[])
{
	const int size= 11;
	double x[] = {0,  1,  2,  3,  4,  5,  6,   7,   8,   9,   10};
	double y[] = {1,  6,  17, 34, 57, 86, 121, 162, 209, 262, 321};
	//double y[] = {10, 10 ,10 ,10 ,10 ,10 ,10 ,10 ,10 ,10 ,10};


	double sum_x=0, sum_y=0, sum_x_square=0, sum_x_cube=0, sum_x_quad=0, sum_x_times_y=0, sum_x_sq_times_y=0;

	for(int i=0;i<size;i++)
	{
		sum_x+=x[i];
		sum_y+=y[i];
		double xsq=x[i]*x[i];
		sum_x_square+=xsq;
		sum_x_cube+=xsq*x[i];
		sum_x_quad+=xsq*xsq;
		sum_x_times_y+=x[i]*y[i];
		sum_x_sq_times_y+=xsq*y[i];


	}


	double a=((sum_x_sq_times_y*sum_x_square)-(sum_x_times_y*sum_x_cube));
	a/=((sum_x_square*sum_x_quad)-sum_x_cube*sum_x_cube);

	double b=((sum_x_times_y*sum_x_quad)-(sum_x_sq_times_y*sum_x_cube));
	b/=((sum_x_square*sum_x_quad)-(sum_x_cube*sum_x_cube));

	double c=sum_y/size - b*sum_x/size - a*sum_x_square/size;

	std::cout <<"Result=" <<a<<"*x*x + "<<b<<"*x +"<<c<<std::endl;

	for(int i=0;i<size;i++)
	{
		double reg=a*x[i]*x[i]+b*x[i]+c;
		std::cout << "x="<<x[i]<< " y="<<y[i]<<" reg="<<reg<<std::endl;
	}
}
