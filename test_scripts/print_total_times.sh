./clean_rep_times.sh $1
echo "encode times"
./print_encode_times.sh $1
echo ""
echo "transmit  times"
./print_transmit_times.sh $1
echo ""
echo "decode times"
./print_decode_times.sh $1
echo ""
echo "create actor times"
./print_create_actor_times.sh $1
echo ""
echo "total times"
find $1/* -type f -print0 | sort -z -t/ -k2 -n | xargs -0 sed -n '5~5p'
