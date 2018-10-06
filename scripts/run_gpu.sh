#Functions to run gpus queries

#run the gpu version
#Parameters:
#	- Binary file path
#	- Window Length
#	- Window Slide
#	- Rate
#	- Num tuples
#	- Results directory (directory in which save the results; it should exist)
function run_financial_gpu {
	BIN=$1
	WL=$2
	WS=$3
	RATE=$4
	NUM_TUPLES=$5
	RES_DIR=$6
	BASEN=${BIN##*/}
	FILENAME="$RES_DIR"/"$BASEN"_"$WL"_"$WS".dat
	echo -e "#NR\tBATCH\tTH" >$FILENAME
	rm tmp_thr 2> /dev/null
	touch tmp_thr
	for ((i=0;i<NUM_RUN;i++))
	do
			echo "Executing Financial GPU ($WL, $WS) with different configurations"
			$BIN $WL $WS 1 1 $RATE $NUM_TUPLES > /tmp/out_prog;

			grep -v "#" throughput.dat | cut -f 3 | paste - tmp_thr > tmp_t
			mv tmp_t tmp_thr

			if [ -f manager.log ]; then     #save manager file if it exists
				mv manager.log manager_bruteforce_"$W_LENGTH"_"$W_SLIDE"_"$R"_"$i".log
			fi
	done    #NUM_RUN
	#adjust measured value by multiplying it by the WSlide
	awk -v s="$WS" '{for (i=1; i<=NF; i++){printf "%d\t",$i*s;} printf "\n"}' tmp_thr > tmp_t
	mv tmp_t tmp_thr

	#compute average per row
	awk '{ s = 0; for (i = 1; i <= NF; i++) s += $i; print s/NF; }' tmp_thr > tmp_t

	grep -v "#" throughput.dat | cut -f 1,2 | paste - tmp_t > $FILENAME
	#cleanup
	rm tmp* 2> /dev/null;
}

#run the gpu version
#Parameters:
#	- Binary file path
#	- Window Length
#	- Window Slide
#	- Num tuples
#	- Acceleration factor
#	- Results directory (directory in which save the results; it should exist)
function run_soccer_gpu {
	BIN=$1
	WL=$2
	WS=$3
	NUM_TUPLES=$4
	ACC=$5
	RES_DIR=$6
	BASEN=${BIN##*/}
	FILENAME="$RES_DIR"/"$BASEN"_"$WL"_"$WS".dat
	echo -e "#NR\tBATCH\tTH" >$FILENAME
	rm tmp_thr 2> /dev/null
	touch tmp_thr
	for ((i=0;i<NUM_RUN;i++))
	do
			echo "Executing Soccer GPU ($WL, $WS) with different configurations"
			$BIN ../wf_gpu/soccer/data_39229326_bin $WL $WS 1 1 $NUM_TUPLES $ACC > /tmp/out_prog;

			grep -v "#" throughput.dat | cut -f 3 | paste - tmp_thr > tmp_t
			mv tmp_t tmp_thr

			if [ -f manager.log ]; then     #save manager file if it exists
				mv manager.log manager_bruteforce_"$W_LENGTH"_"$W_SLIDE"_"$R"_"$i".log
			fi
	done    #NUM_RUN
	#adjust measured value by multiplying it by the WSlide
	awk -v s="$WS" '{for (i=1; i<=NF; i++){printf "%d\t",$i*s;} printf "\n"}' tmp_thr > tmp_t
	mv tmp_t tmp_thr

	#compute average per row
	awk '{ s = 0; for (i = 1; i <= NF; i++) s += $i; print s/NF; }' tmp_thr > tmp_t

	grep -v "#" throughput.dat | cut -f 1,2 | paste - tmp_t > $FILENAME
	#cleanup
	rm tmp* 2> /dev/null;
}
