# Script: max rate sustainable by GASWOP
#Usage: gnuplot -e "Q1='<path>'; Q2='<path>'; OUT_Q1='<path>'; OUT_Q2='<path>'"
set terminal postscript enhanced color
set size ratio 0.35

# grid style
set style line 1 lw 1 lt 0 lc rgb '#808080'
set grid back ls 1

# set legend style
set key top horizontal

# set font size
set xtics font "Helvetica, 16"
set ytics font "Helvetica, 16"
set xlabel font "Helvetica, 18"
set ylabel font "Helvetica, 18"
set title font "Helvetica, 24"
set key font "Helvetica, 20"

# type of the plot
set style data histogram
set style histogram cluster gap 1.5
set style fill solid 0.5 border 0
set boxwidth 0.9 relative
set offset -0.4,-0.4,0,0

# lines of the plot
set style line 2 lc rgb 'blue' lt 1 lw 3
set style line 3 lc rgb '#800000' lt 1 lw 3
set style line 4 lc rgb '#FFB266' lt 1 lw 3
set style line 5 lc rgb '#19860D' lt 1 lw 3
set style line 6 lc rgb "red" lt 15 lw 8


# plot Q1
set yrange [0:3000000]
set ytics ("0.5M" 500000, "1M" 1000000, "1.5M" 1500000, "2M" 2000000, "2.5M" 2500000, "3M" 3000000)
set ylabel "Max input rate (tuples/sec)" offset -1,0,0
set output OUT_Q1
set title "CPU vs. GPU comparison with Q1" offset 0,-1,0
plot Q1 u 2:xtic(1) ls 2 title "CPU", '' using 3 ls 3 fs pattern 1 title "GPU"

# plot Q2
set yrange [0:1250000]
set ytics ("0.25M" 250000, "0.5M" 500000, "0.75M" 750000, "1M" 1000000, "1.25M" 1250000)
set ylabel "Max input rate (tuples/sec)" offset -1,0,0
set output OUT_Q2
set title "CPU vs. GPU comparison with Q2" offset 0,-1,0
plot Q2 u 2:xtic(1) ls 2 title "CPU", '' using 3 ls 3 fs pattern 1 title "GPU"

