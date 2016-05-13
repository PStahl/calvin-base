./clean_rep_times.sh $1
find $1/* -type f -print0 | sort -z -t/ -k2 -n | xargs -0 sed -n '4~5p'
