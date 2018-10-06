# ***************************************************************************
#
#  This program is free software; you can redistribute it and/or modify it
#  under the terms of the GNU Lesser General Public License version 3 as
#  published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful, but WITHOUT
#  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
#   FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
#   License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with this program; if not, write to the Free Software Foundation,
#  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# ****************************************************************************
#
# Author: Tiziano De Matteis <dematteis at di.unipi.it>


DELAUNAY_DIR	= delaunay_linterp
FF_ROOT			= fastflow
DELAUNAY_REPO	= https://github.com/rncarpio/delaunay_linterp
FF_REPO			= https://github.com/fastflow/fastflow

CXX				= g++
INCLUDES		= -I $(FF_ROOT) -I $(PWD)/include
CXXFLAGS		= -std=c++11 -O3

NCXX			= nvcc
NCXXFLAGS		= -x cu -w -std=c++11 -O3 --expt-extended-lambda -gencode arch=compute_35,code=sm_35
NCLIBS			= -lCGAL -lgmp -lgsl -lgslcblas
LDFLAGS			= -pthread
MACROS			= -DPARTIAL
TARGETS			= test_wf test_wf_gpu test_template financial



.DEFAULT_GOAL := all
.PHONY: all clean cleanall delaunay fastflow
.SUFFIXES: .cpp


all: bin financial_cpu financial_gpu soccer_cpu soccer_gpu

#Financial

financial_cpu: fastflow delaunay src/financial_cpu.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) $(OPTFLAGS) -o bin/$@ src/financial_cpu.cpp $(LDFLAGS)

financial_gpu: fastflow delaunay  src/financial_gpu.cpp
	$(NCXX) $(NCXXFLAGS) $(INCLUDES) $(MACROS) -o bin/$@  src/financial_gpu.cpp $(NCLIBS)

#######SOCCER

soccer_cpu: fastflow delaunay  src/soccer_cpu.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) $(OPTFLAGS) -o bin/$@ src/soccer_cpu.cpp $(LDFLAGS)

soccer_gpu: fastflow delaunay  src/soccer_gpu.cpp include/Win_GPU_Config.h
	$(NCXX) $(NCXXFLAGS) $(INCLUDES) $(MACROS) -o bin/$@ src/soccer_gpu.cpp $(NCLIBS)


### Phony

fastflow:
	@if [ ! -d $(FF_ROOT) ] ;\
	then \
	  echo "FastFlow does not exist, fetching"; \
	  git clone $(FF_REPO); \
	fi

delaunay:
	@if [ ! -d $(DELAUNAY_DIR) ] ;\
	then \
	  echo "Delaunay_linterp does not exist, fetching"; \
	  git clone $(DELAUNAY_REPO); \
	fi

bin:
	mkdir bin

clean:
	rm -f bin/*

cleanall:
	rm -f bin/*.o bin/*~


