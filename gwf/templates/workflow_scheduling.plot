set xlabel "Time (Relative to Start)"
set ylabel "No. Tasks"
set grid y
set border 3
set xtics 1
set ytics 1

label(x) = sprintf("%.3f",column(x))

set style fill solid noborder
set style data filledcurves y1=0
set clip two

set key below

set terminal png enhanced
set output outfile

plot infile using ($2+$3+$4+$5):xticlabels(label(1)) title "queued", \
     infile using ($3+$4+$5):xticlabels(label(1)) title "running", \
     infile using ($4+$5):xticlabels(label(1)) title "completed", \
     infile using 5:xticlabels(label(1)) title "failed"
