[ -f calvin.conf.bak ] && mv calvin.conf.bak calvin.conf
echo "Starting runtime on port $1 - $2"
#hostname=`hostname -A | awk '{print $1}'`
#IP=`getent hosts $hostname | awk '{ print $1 }'`
IP=`ip route get 8.8.8.8 | awk '{print $NF; exit}'`
csruntime --host $IP --port $1 --controlport $2 --keep-alive
# 'calvin.runtime.north.lost_node_handler:DEBUG' -l 'calvin.runtime.north.replicator:DEBUG' # -l 'calvin.runtime.north.portmanager:DEBUG' -l 'calvin.runtime.north.calvin_network:DEBUG'
# -l 'calvin.runtime.north.calvin_proto:DEBUG'
# -f "`hostname -A | awk '{print $1}'`$2.log" 
# -l 'calvin.runtime.north.scheduler:DEBUG' # -l 'calvin.actor.actor:DEBUG'
#-l ERROR
