[ -f calvin.conf ] && mv calvin.conf calvin.conf.bak

n=$1
if [ -z "$n" ]; then
    n=0
fi
port=$((4999+$n))
controlport=$(($port+1))

csruntime --host localhost --port $port --controlport $controlport --keep-alive -s
