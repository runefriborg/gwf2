unset key

set ylabel "Time (Relative to Start)"
set grid y
set border 3
set xrange [-.5:]
set style data histograms
set style histogram errorbars gap 0 lw 2
set xtics 1

set terminal png
set output outfile

plot infile
