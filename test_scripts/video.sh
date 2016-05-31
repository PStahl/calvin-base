node=`cscontrol http://localhost:5002 nodes add calvinip://localhost:5004`
#node=`cscontrol http://localhost:5002 nodes add calvinip://localhost:5006`
#node2=`cscontrol http://localhost:5002 nodes add calvinip://localhost:5004`
a=`cscontrol http://localhost:5002 deploy ../calvin/examples/video_encoding/video_encoding.calvin --reqs args`
echo $a
echo ""
echo $node
echo ""
echo $node_2

snk_id=`echo $a | perl -nle"print $& if m{(?<=snk': u').*?(?=')}"`
encoder_id=`echo $a | perl -nle"print $& if m{(?<=encoder': u').*?(?=')}"`

node_id=`echo $node | perl -nle"print $& if m{(?<=127.0.0.1:5004': \[u').*?(?=')}"`
node_id_2=`echo $node_2 | perl -nle"print $& if m{(?<=127.0.0.1:5004': \[u').*?(?=')}"`

echo "node"
echo $node_id
echo "encoder"
echo $encoder_id

#echo $encoder_id
#echo $a
cscontrol http://localhost:5002 actor migrate $encoder_id $node_id
#cscontrol http://localhost:5002 actor migrate $encoder_id $node_id
