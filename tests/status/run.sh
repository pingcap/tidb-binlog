#!/bin/sh

 set -e

 cd "$(dirname "$0")"

# download binlogctl
wget download.pingcap.org/tidb-tools-v2.1.0-linux-amd64.tar.gz
tar zxvf tidb-tools-v2.1.0-linux-amd64.tar.gz

statusLog="status.log"
./tidb-tools-v2.1.0-linux-amd64/bin/binlogctl -pd-urls 127.0.0.1:2379 -cmd pumps > $statusLog
if ! grep -Fq "online" $statusLog; then
    echo "pump is not online" 
	exit 2
fi

./tidb-tools-v2.1.0-linux-amd64/bin/binlogctl -pd-urls 127.0.0.1:2379 -cmd stop-pump -node-id pump1:8215
sleep 2
./tidb-tools-v2.1.0-linux-amd64/bin/binlogctl -pd-urls 127.0.0.1:2379 -cmd pumps > $statusLog
if ! grep -Fq "paused" $statusLog; then
    echo "pump is not paused"
	exit 2
fi

run_drainer &

sleep 2

killall drainer || true

./tidb-tools-v2.1.0-linux-amd64/bin/binlogctl -pd-urls 127.0.0.1:2379 -cmd stop-drainer -node-id drainer:123

sleep 2

./tidb-tools-v2.1.0-linux-amd64/bin/binlogctl -pd-urls 127.0.0.1:2379 -cmd drainers > $statusLog
if ! grep -Fq "paused" $statusLog; then
    echo "drainer is not paused"
	exit 2
fi

./tidb-tools-v2.1.0-linux-amd64/bin/binlogctl -pd-urls 127.0.0.1:2379 -cmd update-drainer -node-id drainer:123 -state online

run_pump

if [ "$?" -e "0" ]; then
    echo "pump should exit with code 2"
    exit 2
fi

# clean up
./tidb-tools-v2.1.0-linux-amd64/bin/binlogctl -pd-urls 127.0.0.1:2379 -cmd update-drainer -node-id drainer:123 -state paused
run_pump &
rm download.pingcap.org/tidb-tools-v2.1.0-linux-amd64.tar.gz
rm -r tidb-tools-v2.1.0-linux-amd64
rm $statusLog

