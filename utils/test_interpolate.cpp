/**
 * TEST
 * Computes the accuracy of an interpolation in D dimension (at the moment n = 2).
 * Loads data from the file specified in argv[1]. File contains one
 * column for each dimension plus a column for the result.
 * It uses 'delaunay_linterp' library (http://rncarpio.github.io/delaunay_linterp/)
 *
 * It requires:
 * - boost
 * - CGAL
 * - eigen3 (probably you will have to edit some include in delaunay_linterp)
 *
 * for compiling
 *  g++ test_interpolate.cpp -lCGAL -lgmp
 **/
#include <cmath>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <algorithm>

#include "../delaunay_linterp/src/delaunay_2_interp.h"

void loadData(const std::string& fileName, 
			  std::vector<double>& xs, 
			  std::vector<double>& ys, 
			  std::vector<double>& zs,
			  bool onlyAllowed = false,
			  std::vector<double>* allowedX = NULL,
			  std::vector<double>* allowedY = NULL){
	//#BatchSize NumWorkers Rq
    std::ifstream fin(fileName);
    double x, y, z;
    while(fin >> x >> y >> z){
    	if(!onlyAllowed || (std::find(allowedX->begin(), allowedX->end(), x) != allowedX->end() &&
    	   	         	    std::find(allowedY->begin(), allowedY->end(), y) != allowedY->end())){
       		xs.push_back(x);
       		ys.push_back(y);
       		zs.push_back(z);
		}
    }
}

double getMin(const std::vector<double>& x){
	double min = std::numeric_limits<double>::max();
	for(double d : x){
		if(d < min){
			min = d;
		}
	}
	return min;
}

double getMax(const std::vector<double>& x){
	double max = std::numeric_limits<double>::min();
	for(double d : x){
		if(d > max){
			max = d;
		}
	}
	return max;
}

double findZ(const std::vector<double>& xs, const std::vector<double>& ys, const std::vector<double>& zs, double x, double y){
	for(size_t i = 0; i < xs.size(); i++){
		if(xs[i] == x && ys[i] == y){
			return zs[i];
		}
	}
}

//#define DETERMINISTIC

int main(int argc, char** argv){
	if(argc < 3){
		std::cerr << "Usage: " << argv[0] << " fileName percTraining" << std::endl;
		return -1;
	}
	// Il file originale con tutti i dati, ne usiamo
	// solo una parte per il "traning". La validazione 
	// poi viene fatta su tutti.
	char* fileName = argv[1];
	double percData = atoi(argv[2]);
	std::vector<double> xs, ys, zs;
	Delaunay_incremental_interp_2 adaptive_triang;
	std::array<double, 2> args;
	srand(time(NULL));
	loadData(fileName, xs, ys, zs);
	// Insert boundary points
	args[0] = xs.front(); args[1] = ys.front(); adaptive_triang.insert(args.begin(), args.end(), findZ(xs, ys, zs, args[0], args[1]));
	args[0] = xs.front(); args[1] = ys.back(); adaptive_triang.insert(args.begin(), args.end(), findZ(xs, ys, zs, args[0], args[1]));
	args[0] = xs.back(); args[1] = ys.front(); adaptive_triang.insert(args.begin(), args.end(), findZ(xs, ys, zs, args[0], args[1]));
	args[0] = xs.back(); args[1] = ys.back(); adaptive_triang.insert(args.begin(), args.end(), findZ(xs, ys, zs, args[0], args[1]));

	uint loaded = 0;
	for(size_t i = 0; i < xs.size(); i++){
#ifdef DETERMINISTIC
		if(i % (int)(100/(float)percData) == 0){
#else
		if(rand() % 100 <= percData){
#endif		
			++loaded;
			args[0] = xs[i];
			args[1] = ys[i];
			adaptive_triang.insert(args.begin(), args.end(), zs[i]);
		}
	}

	std::cout << "Caricati: " << loaded << "(" << loaded/(double)xs.size() * 100.0 << "%)" << std::endl;
	/*
	for (int i=0; i<n_points*n_points-4; i++) {
		adaptive_triang.insert_largest_error_point();
	} 
	*/

	double percError = 0;
	double maxError = std::numeric_limits<double>::min();
	double minError = std::numeric_limits<double>::max();
	for(size_t i = 0; i < xs.size(); i++){
		array<double, 2> args = {xs[i], ys[i]};
		double predictedValue = adaptive_triang.interp(args.begin(), args.end());
		double currentError = std::abs((predictedValue - zs[i])/zs[i]);
		std::cout << currentError << std::endl;
		percError += std::abs(currentError);
		if(currentError < minError){
			minError = currentError;
		}
		if(currentError > maxError){
			maxError = currentError;
		}
	}
	std::cout << "Average error [0, 1]: " << percError / xs.size() << std::endl;
	std::cout << "Minimum error [0, 1]: " << minError << std::endl;
	std::cout << "Maximum error [0, 1]: " << maxError << std::endl;

	return 0;
}
