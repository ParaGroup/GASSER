# GASPOW
An Autotunable System for Sliding-Window Non-Incremental Streaming Operators on GPU


## Requirements

### For building Gaspow

* gcc (>4.8)
* cuda (>=8)
* gsl (>=2)

Additionally Gaspow depends from FastFlow (http://calvados.di.unipi.it/)  and Dalaunay_linterp (http://rncarpio.github.io/delaunay_linterp/). These are header only libraries and are automatically downloaded during the building process but have additional requirements.

### Required by Delaunay_linterp
Minimum requirements (see http://rncarpio.github.io/delaunay_linterp/ for the full requirements) are:
* CGAL
* GMP
* Eigen