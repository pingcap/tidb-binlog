#!/bin/sh

 set -e

 cd "$(dirname "$0")"

# download binlogctl
wget download.pingcap.org/tidb-tools-v2.1.0-linux-amd64.tar.gz
tar zxvf tidb-tools-v2.1.0-linux-amd64.tar.gz

statusLog="status.log"
# pump's state should be online
./tidb-tools-v2.1.0-linux-amd64/bin/binlogctl -pd-urls 127.0.0.1:2379 -cmd pumps > $statusLog 2>&1
cat $statusLog
if ! grep -Fq "online" $statusLog; then
    echo "pump is not online" 
	exit 2
fi

# stop pump, and pump's state should be paused
./tidb-tools-v2.1.0-linux-amd64/bin/binlogctl -pd-urls 127.0.0.1:2379 -cmd pause-pump -node-id pump1:8215
sleep 5
./tidb-tools-v2.1.0-linux-amd64/bin/binlogctl -pd-urls 127.0.0.1:2379 -cmd pumps > $statusLog 2>&1
cat $statusLog
if ! grep -Fq "paused" $statusLog; then
    echo "pump is not paused"
	exit 2
fi

run_drainer &

sleep 2

./tidb-tools-v2.1.0-linux-amd64/bin/binlogctl -pd-urls 127.0.0.1:2379 -cmd drainers > $statusLog 2>&1
cat $statusLog
if ! grep -Fq "online" $statusLog; then
    echo "drainer is not online"
	exit 2
fi

nodeid=`cat $statusLog | sed 's/.*NodeID:\([a-zA-Z0-9]*:[0-9]*\) .*/\1/g'`
./tidb-tools-v2.1.0-linux-amd64/bin/binlogctl -pd-urls 127.0.0.1:2379 -cmd pause-drainer -node-id $nodeid

sleep 2

# stop drainer, and drainer's state should be paused
./tidb-tools-v2.1.0-linux-amd64/bin/binlogctl -pd-urls 127.0.0.1:2379 -cmd drainers > $statusLog 2>&1
cat $statusLog
if ! grep -Fq "paused" $statusLog; then
    echo "drainer is not paused"
	exit 2
fi

killall pump || true

# update drainer's state to online, and then run pump, pump should exit with error.
./tidb-tools-v2.1.0-linux-amd64/bin/binlogctl -pd-urls 127.0.0.1:2379 -cmd update-drainer -node-id $nodeid -state online

run_pump

if [ "$?" -e "0" ]; then
    echo "pump should exit with code 2"
    exit 2
fi

# clean up
./tidb-tools-v2.1.0-linux-amd64/bin/binlogctl -pd-urls 127.0.0.1:2379 -cmd update-drainer -node-id $nodeid -state paused
run_pump &
rm download.pingcap.org/tidb-tools-v2.1.0-linux-amd64.tar.gz
rm -r tidb-tools-v2.1.0-linux-amd64
rm $statusLog

