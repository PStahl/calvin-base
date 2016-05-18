[ -f calvin.conf.bak ] && mv calvin.conf.bak calvin.conf
hostname=`hostname -A | awk '{print $1}'`
IP=`getent hosts $hostname | awk '{ print $1 }'`
csruntime --host $IP --port 5001 --controlport 5002 --keep-alive  -f "`hostname -A | awk '{print $1}'`.log" # -l DEBUG
# 'calvin.runtime.north.replicator:DEBUG' -l 'calvin.runtime.north.lost_node_handler:DEBUG' -l 'calvin.runtime.north.portmanager:INFO' -l 'calvin.runtime.north.calvin_proto:DEBUG' # -l 'calvin.runtime.north.resource_manager:DEBUG' # -l 'calvin.runtime.north.calvin_network:DEBUG'
