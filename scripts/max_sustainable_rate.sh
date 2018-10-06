#!/bin/bash
#to be called in the gaspow directory
NUM_RUN=1
MAX_PAR_DEG=11
RATE=3000000					#input rate for Query1
ACC=100							#acceleration factor for Query2
#num tuples for Query1
NUM_TUPLES_CPU=2000000
NUM_TUPLES_GPU=300000000		#for the gpu we need more tuples sincewe evaluate all the configurations
#num tuples for Query2
NUM_TUPLES_SOCCER_CPU=1000000
NUM_TUPLES_SOCCER_GPU=150000000
RES_DIR=results
PLOT_DIR=plots
declare -a W_L=("10000" "25000" "50000")
declare -a W_S=("5" "25" "100")


source scripts/run_cpu.sh;
source scripts/run_gpu.sh;

mkdir $RES_DIR 2> /dev/null;

#remove previous results if present
rm  $RES_DIR/max_rate_financial.dat 2> /dev/null;
rm  $RES_DIR/max_rate_soccer.dat 2> /dev/null;

#For every combination we will find the maximum rate sustainable with the cpu and the gpu
#and put the result in the proper file

for W_LENGTH in "${W_L[@]}"
do

		for W_SLIDE in "${W_S[@]}"
		do

			run_financial_cpu bin/financial_cpu $W_LENGTH $W_SLIDE $RATE $NUM_TUPLES_CPU $RES_DIR;
			run_financial_gpu bin/financial_gpu $W_LENGTH $W_SLIDE $RATE $NUM_TUPLES_GPU $RES_DIR;

			#compute the maximum for the cpu
			tail -n+2 "$RES_DIR"/financial_cpu_"$W_LENGTH"_"$W_SLIDE".dat| awk 'BEGIN{max=0}{if ($2>max) max=$2}END{print max}' > /tmp/gaspow_cpu
			#compute the maximum for the gpu
			tail -n+2 "$RES_DIR"/financial_gpu_"$W_LENGTH"_"$W_SLIDE".dat| awk 'BEGIN{max=0}{if ($3>max) max=$3}END{print max}' > /tmp/gaspow_gpu
			echo "\"w=$W_LENGTH\n s=$W_SLIDE\"" | paste - /tmp/gaspow_cpu /tmp/gaspow_gpu >> $RES_DIR/max_rate_financial.dat

			run_soccer_cpu bin/soccer_cpu $W_LENGTH $W_SLIDE $NUM_TUPLES_SOCCER_CPU $ACC $RES_DIR;
			run_soccer_gpu bin/soccer_gpu $W_LENGTH $W_SLIDE $NUM_TUPLES_SOCCER_GPU $ACC $RES_DIR;

			#compute the maximum for the cpu
			tail -n+2 "$RES_DIR"/soccer_cpu_"$W_LENGTH"_"$W_SLIDE".dat| awk 'BEGIN{max=0}{if ($2>max) max=$2}END{print max}' > /tmp/gaspow_cpu
			#compute the maximum for the gpu
			tail -n+2 "$RES_DIR"/soccer_gpu_"$W_LENGTH"_"$W_SLIDE".dat| awk 'BEGIN{max=0}{if ($3>max) max=$3}END{print max}' > /tmp/gaspow_gpu
			echo  "\"w=$W_LENGTH\n s=$W_SLIDE\"" | paste - /tmp/gaspow_cpu /tmp/gaspow_gpu >> $RES_DIR/max_rate_soccer.dat


		done

done

#generate plots
mkdir $PLOT_DIR 2> /dev/null;
cp scripts/crop.sh $PLOT_DIR/
#plot the results
gnuplot -e "Q1='${RES_DIR}/max_rate_financial.dat'; Q2='${RES_DIR}/max_rate_soccer.dat'; OUT_Q1='${PLOT_DIR}/max_rate_Q1.eps';OUT_Q2='${PLOT_DIR}/max_rate_Q2.eps'" scripts/plot_max_rate.plt
pushd $PLOT_DIR
bash crop.sh
popd
