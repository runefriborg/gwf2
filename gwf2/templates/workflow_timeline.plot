unset key

set ylabel "Time (Relative to Start)"
set grid y
set border 3
set xrange [-.5:]
set style data histograms
set style histogram errorbars gap 1 lw 2

set xtics right rotate by 45

set terminal png
set output outfile

plot infile using 2:3:4:xticlabels(1)
