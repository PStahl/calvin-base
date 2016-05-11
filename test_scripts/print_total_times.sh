./clean_rep_times.sh
find rep_times/* -type f -print0 | sort -z -t/ -k2 -n | xargs -0 sed -n '5~5p'
