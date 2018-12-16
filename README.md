# GASSER
An Autotunable System for Sliding-Window Non-Incremental Streaming Operators on GPU

*(Please note that this is a prelimnary version of the repository)*
## Requirements



### For building Gasser

* gcc (>4.8)
* cuda (>=8)
* gsl (>=2)

Additionally Gasser depends from FastFlow (http://calvados.di.unipi.it/)  and Dalaunay_linterp (http://rncarpio.github.io/delaunay_linterp/). These are header only libraries and are automatically downloaded during the building process but have additional requirements.

### Required by Delaunay_linterp
Minimum requirements (see http://rncarpio.github.io/delaunay_linterp/ for the full requirements) are:

* CGAL
* GMP
* Eigen


## Building

Before building the programs, the user must *manually* edit the configuration file  *config/machine_config.h"* detailing the characteristic of the execution platform.
In particular, he/she must indicates:

* `SM`, the  number of streaming multiprocessor of the used GPU;
*  `CORE_PER_SM`, the numer of cuda core per streaming multiprocessor;
*  `REPLICA`, the number of the CPU replicas to use.

Please, be aware that all these parameters impacts performance. Read the instruction in the header file on how to choose them.


After this, to compile it is sufficient to run:

```
    $ make all
```

The produced binaries will be placed under the `bin` directory:

* `financial_cpu` and `financial_gpu` represent respectively the *cpu* (implemented using FastFlow and Posix thread) and *gpu* (implemented with Gasser) version of the financial use case (query 1 in the paper);
* `soccer_cpu` and `soccer_gpu` represent the *cpu* (FastFlow and Posix) and the *gpu* version (Gasser) of the soccer use case (query 2 in the paper).


**Please note**: the gpu versions are by default compiled with the raindrop heuristic. If you want to let the Manager tries exhaustively the different configuration, you can compile the programs with the `BRUTE_FORCE` preprocessor macrod defined. It can be enabled also by uncommenting 
line 46 in the file `include/Win_GPU_Config.h`

### Known problems
When compiling you can receive an error regarding the `delaunay_linter` library:

```
delaunay_2_interp.h:39:23: fatal error: Eigen/Dense: No such file or directory
```

this could occur depending on your Linux distribution and installation path. A possible solution could be to edit the indicated file (under ` delaunay_linterp/src/delaunay_2_interp.h`) and change the line 39 with the proper installation path of the Eigen library

## Executing

### Financial use case

For the Financial Scenario, the inter-arrival times between tuples are generated using
an exponential distribution with a configurable mean in order to control the input intensity of the stream.

To launch the financial use case:





## Reproducing results

### Max Sustainable Rate 



