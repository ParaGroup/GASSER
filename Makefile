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


FF_ROOT		= $(HOME)/fastflow

CXX			= g++
INCLUDES	= -I $(FF_ROOT) -I $(PWD)/include
CXXFLAGS  	= -std=c++11 -O3

NCXX		= nvcc
NCXXFLAGS	= -x cu -w -std=c++11 -O3 --expt-extended-lambda -gencode arch=compute_35,code=sm_35
NCLIBS      = -lCGAL -lgmp -lgsl -lgslcblas


LDFLAGS 	= -pthread
MACROS		= -DPARTIAL
TARGETS		= test_wf test_wf_gpu test_template financial

.DEFAULT_GOAL := all
.PHONY: all clean cleanall
.SUFFIXES: .cpp




#Financial

financial_cpu: src/financial_cpu.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) $(OPTFLAGS) -o bin/$@ $^ $(LDFLAGS)

financial_gpu: src/financial_gpu.cpp
	$(NCXX) $(NCXXFLAGS) $(INCLUDES) $(MACROS) -o bin/$@ $^ $(NCLIBS)

#######SOCCER

soccer_cpu: src/soccer_cpu.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) $(OPTFLAGS) -o bin/$@ $^ $(LDFLAGS)

soccer_gpu: src/soccer_gpu.cpp include/Win_GPU_Config.h
	$(NCXX) $(NCXXFLAGS) $(INCLUDES) $(MACROS) -o bin/$@ $^ $(NCLIBS)


clean:
	rm -f bin/*

cleanall:
	rm -f bin/*.o bin/*~
