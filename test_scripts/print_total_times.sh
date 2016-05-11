./clean_rep_times.sh
echo ""
./print_encode_times.sh
echo ""
./print_transmit_times.sh
echo ""
./print_decode_times.sh
echo ""
./print_create_actor_times.sh
echo ""
find rep_times/* -type f -print0 | sort -z -t/ -k2 -n | xargs -0 sed -n '5~5p'
