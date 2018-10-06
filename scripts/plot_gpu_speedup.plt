# Script :speedup GPU vs CPU.
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

# plot 1Mt/s (Q1)

set yrange [1:35]
set ylabel "GPU Speedup" offset 1,0,0
set xlabel "Sliding factor"
set log y
set ytics 5
set key title "Window length"
set output OUT_Q1
set title "GPU Speedup, query Q1 - Input Rate 1Mt/s" offset 0,-1,0
plot Q1 u 2:xtic(1) ls 2 title "25K", '' using 3 ls 3 fs pattern 1 title "50K", '' using 4 ls 4 fs pattern 2 title "100K"

# plot 1Mt/s (Q2)
set output OUT_Q2
set title "GPU Speedup, query Q2 - Input Rate 1Mt/s" offset 0,-1,0
plot Q2 u 2:xtic(1) ls 2 title "25K", '' using 3 ls 3 fs pattern 1 title "50K", '' using 4 ls 4 fs pattern 2 title "100K"


