node=`cscontrol http://127.0.0.1:5002 nodes add calvinip://127.0.0.1:5004`
#node2=`cscontrol http://127.0.0.1:5002 nodes add calvinip://127.0.0.1:5004`
a=`cscontrol http://127.0.0.1:5002 deploy ../calvin/examples/sample-scripts/actions.calvin --reqs args`
echo $a
echo ""
echo $node
echo ""
echo $node_2

snk_id=`echo $a | perl -nle"print $& if m{(?<=snk': u').*?(?=')}"`
src_id=`echo $a | perl -nle"print $& if m{(?<=src': u').*?(?=')}"`

node_id=`echo $node | perl -nle"print $& if m{(?<=127.0.0.1:5004': \[u').*?(?=')}"`
echo $node
node_id_2=`echo $node_2 | perl -nle"print $& if m{(?<=127.0.0.1:5004': \[u').*?(?=')}"`

echo "node"
echo $node_id
echo "src"
echo $src_id

cscontrol http://127.0.0.1:5002 actor migrate $src_id $node_id
#cscontrol http://127.0.0.1:5002 actor migrate $src_id $node_id
