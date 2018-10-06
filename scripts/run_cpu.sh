#run methods for cpu based queries
#they are used in other files in which are defined the MAX_PAR_DEG and NUM_RUN variables

#function to compute averaged values over runs
#Results are printed on stdout
function compute_average {
	cat $1 | sed  '/^$/d' | awk 'BEGIN{sum=0; sumsq=0;}
	{sum+=$1; sumsq+=$1*$1;}
	 END{
			AVG=sum/NR;
			STDDEV=sqrt(sumsq/NR - (sum/NR)*(sum/NR));
			STDERR=STDDEV/sqrt(AVG);
		   print AVG,"\t"STDDEV;
	  }'
}

#call the cpu version of the first query
#Parameters:
#	- Binary file path
#	- Window Length
#	- Window Slide
#	- Rate
#	- Num tuples
#	- Results directory (directory in which save the results; it should exist)
function run_financial_cpu {
	BIN=$1
	WL=$2
	WS=$3
	RATE=$4
	NUM_TUPLES=$5
	RES_DIR=$6
	BASEN=${BIN##*/}
	FILENAME="$RES_DIR"/"$BASEN"_"$WL"_"$WS".dat
	echo -e "#NR\tTH\tSTDEV" >$FILENAME
	for ((j=8;j<=MAX_PAR_DEG;j++))
	do
		rm tmp_prog	2> /dev/null;
		for ((i=0;i<NUM_RUN;i++))
		do
			echo "Executing Financial CPU ($WL, $WS) with $j replicas"
			$BIN $WL $WS $j $RATE $NUM_TUPLES > /tmp/out_prog;
			#get the throughput: it is printed on stdout
			cat /tmp/out_prog | grep processed | cut -f 2 -d ":"| awk -v s="$WS" '{print $1*s}'  >> tmp_prog
		done	#NUM_RUN

		#compute averaged values over runs
		echo -ne "$j\t"  >> $FILENAME
		compute_average tmp_prog  >> $FILENAME
	done

	rm tmp_prog;

}

#Parameters:
#	- Binary file path
#	- Window Length
#	- Window Slide
#	- Num Tuples
#	- Acceleration factor
#	- Results directory (directory in which save the results; it should exist)
function run_soccer_cpu {
	BIN=$1
	WL=$2
	WS=$3
	NUM_TUPLES=$4
	ACC=$5
	RES_DIR=$6
	BASEN=${BIN##*/}
	FILENAME="$RES_DIR"/"$BASEN"_"$WL"_"$WS".dat

	echo -e "#NR\tTH\tSTDEV" >$FILENAME
	for ((j=8;j<=MAX_PAR_DEG;j++))
	do
		rm tmp_prog 2> /dev/null
		for ((i=0;i<NUM_RUN;i++))
		do
				echo "Executing Soccer CPU ($WL, $WS) with $j replicas"
				$BIN ../wf_gpu/soccer/data_39229326_bin $WL $WS $j $NUM_TUPLES $ACC > /tmp/out_prog
				cat /tmp/out_prog | grep processed | cut -f 2 -d ":"| awk -v s="$WS" '{print $1*s}'  >> tmp_prog
		done    #NUM_RUN
		#compute averaged values over runs
		echo -ne "$j\t"  >> $FILENAME
		compute_average tmp_prog  >> $FILENAME
	done

	rm tmp_prog;

}
