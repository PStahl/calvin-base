#rand=`python -c "import random; print random.randint(30, 90)"`
rands=($(python -c "import numpy as np;
mu=18.9; sigma=1.0;
s = np.random.normal(mu, sigma, 1000)
for n in s:
        print n
"))
i=0

n=$1
if [ -z "$n" ]; then
    n=1
fi
port=$((5000+$n))
controlport=$(($port+1))

host="localhost"

extra=0

while true; do
    rand=${rands[i]}
    echo $rand
    rand=`awk "BEGIN {print ($rand - $extra)}"`
    extra=0
    i=$((i+1))
    echo "random: $rand"

    #[ -f calvin.conf.bak ] && mv calvin.conf.bak calvin.conf
    #echo "Starting runtime on port $port - $controlport"
    #csruntime --host `hostname -A | awk '{print $1}'` --port $port --controlport $controlport --keep-alive -l ERROR -l 'calvin.runtime.north.lost_node_handler:DEBUG' -l 'calvin.runtime.north.replicator:DEBUG' -l 'calvin.runtime.north.portmanager:DEBUG' &
	./start.sh $port $controlport &
    PID=$!
    echo "PID: $PID"

    sleep 0.5
    #echo `ps aux | grep -E "(csruntime|py.test)"`

    #echo "cscontrol http://localhost:5002 nodes add calvinip://$host:$port"
    nodes_add="null"
    regex="null|Read timed out"
    while [[ "$nodes_add" =~ $regex ]];
    do
        #echo "adding node"
        #cscontrol http://localhost:5002 nodes list
        nodes_add=`cscontrol http://localhost:5002 nodes add calvinip://$host:$port`
        echo $nodes_add
    done
    #nodes_add=`cscontrol http://localhost:5002 nodes add calvinip://$host:$port`
    #echo "$nodes_add"
    sleep $rand

    #pkill -9 -f ".*csruntime --host $host --port $port"
    #echo "BEFORE: $before"
    #pstree -p

    printf "\n\n"
    date +"%Y-%m-%d %H:%M:%S.%N"
    printf "\n\n"

    nodes=`cscontrol http://localhost:5002 nodes list`
    n_nodes=$(grep -o " " <<< "$nodes" | wc -l)
    while [ $n_nodes -lt 1 ]
    do
        echo "not enough nodes, sleeping..."
        echo $n_nodes
        nodes=`cscontrol http://localhost:5002 nodes list`
        n_nodes=$(grep -o " " <<< "$nodes" | wc -l)
        sleep 0.2
        extra=`awk "BEGIN {print ($extra + 0.2)}"`
    done
    pkill -9 -P $PID
    sleep 0.2
done
