NUM_RUN=2
MAX_PAR_DEG=11
RATE=1000000					#input rate for Query1
ACC=80							#acceleration factor for Query2 to obtain ~1Mts
#num tuples for Query1
NUM_TUPLES_CPU=20000000
NUM_TUPLES_GPU=500000000		#for the gpu we need more tuples sincewe evaluate all the configurations
#num tuples for Query2
NUM_TUPLES_SOCCER_CPU=10000000
NUM_TUPLES_SOCCER_GPU=300000000
RES_DIR=results
PLOT_DIR=plots
declare -a W_L=("25000" "50000" "100000")
declare -a W_S=("5" "25" "50" "100" "250" "500" "1000" "2500")


source scripts/run_cpu.sh;
source scripts/run_gpu.sh;

mkdir $RES_DIR 2> /dev/null;

#remove previous results if present
rm  $RES_DIR/gpu_speedup_financial.dat 2> /dev/null;
rm  $RES_DIR/gpu_speedup_soccer.dat 2> /dev/null;

#The gpu_speedup_* file will be organized in the following way:
#- on each column we will have a window length
#- on each row we will have a window slide
#- the value is the maximum rate achieved with this configuration

#so we fill the file in this manner

for W_SLIDE in "${W_S[@]}"
do
		echo -ne $W_SLIDE >> $RES_DIR/gpu_speedup_financial.dat
		echo -ne $W_SLIDE >> $RES_DIR/gpu_speedup_soccer.dat

		for W_LENGTH in "${W_L[@]}"
		do

			#Q1

			run_financial_cpu bin/financial_cpu $W_LENGTH $W_SLIDE $RATE $NUM_TUPLES_CPU $RES_DIR;
			run_financial_gpu bin/financial_gpu $W_LENGTH $W_SLIDE $RATE $NUM_TUPLES_GPU $RES_DIR;

			#compute the maximum for the cpu
			max_rate_cpu=$(tail -n+2 "$RES_DIR"/financial_cpu_"$W_LENGTH"_"$W_SLIDE".dat| awk 'BEGIN{max=0}{if ($2>max) max=$2}END{print max}')
			#compute the maximum for the gpu
			max_rate_gpu=$(tail -n+2 "$RES_DIR"/financial_gpu_"$W_LENGTH"_"$W_SLIDE".dat| awk 'BEGIN{max=0}{if ($3>max) max=$3}END{print max}')
			#compute the gpu_speedup
			gpu_speedup=$(echo "scale=2 ; $max_rate_gpu / $max_rate_cpu" | bc)
			echo -ne "\t"$gpu_speedup >> $RES_DIR/gpu_speedup_financial.dat

			#Q2
			run_soccer_cpu bin/soccer_cpu $W_LENGTH $W_SLIDE $NUM_TUPLES_SOCCER_CPU $ACC $RES_DIR;
			run_soccer_gpu bin/soccer_gpu $W_LENGTH $W_SLIDE $NUM_TUPLES_SOCCER_GPU $ACC $RES_DIR;

			#compute the maximum for the cpu
			max_rate_cpu=$(tail -n+2 "$RES_DIR"/soccer_cpu_"$W_LENGTH"_"$W_SLIDE".dat| awk 'BEGIN{max=0}{if ($2>max) max=$2}END{print max}')
			#compute the maximum for the gpu
			max_rate_gpu=$(tail -n+2 "$RES_DIR"/soccer_gpu_"$W_LENGTH"_"$W_SLIDE".dat| awk 'BEGIN{max=0}{if ($3>max) max=$3}END{print max}')
			#compute the gpu_speedup
			gpu_speedup=$(echo "scale=2 ; $max_rate_gpu / $max_rate_cpu" | bc)
			echo -ne "\t"$gpu_speedup >> $RES_DIR/gpu_speedup_soccer.dat


		done #wlength

		echo "" >>  $RES_DIR/gpu_speedup_financial.dat	#newline
		echo "" >>  $RES_DIR/gpu_speedup_soccer.dat	#newline

done

mkdir $PLOT_DIR 2> /dev/null;
cp scripts/crop.sh $PLOT_DIR/
#plot the results
gnuplot -e "Q1='${RES_DIR}/gpu_speedup_financial.dat'; Q2='${RES_DIR}/gpu_speedup_soccer.dat'; OUT_Q1='${PLOT_DIR}/gpu_speedup_Q1.eps';OUT_Q2='${PLOT_DIR}/gpu_speedup_Q2.eps'" scripts/plot_gpu_speedup.plt
pushd $PLOT_DIR
bash crop.sh
popd
