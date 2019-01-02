#!/bin/sh

 set -e

 cd "$(dirname "$0")"

statusLog="status.log"

# run drainer, and drainer's status should be online
run_drainer &
sleep 3
binlogctl -pd-urls 127.0.0.1:2379 -cmd drainers > $statusLog 2>&1
cat $statusLog
count=`grep -c "online" $statusLog`
if [ $count -ne 1 ]; then
     echo "drainer is not online"
    exit 2
fi

drainerNodeID=`cat $statusLog | sed 's/.*NodeID:\([a-zA-Z0-9\-]*:[0-9]*\) .*/\1/g'`

# pump's state should be online
binlogctl -pd-urls 127.0.0.1:2379 -cmd pumps > $statusLog 2>&1
cat $statusLog
count=`grep -c "online" $statusLog`
if [ $count -ne 1 ]; then
     echo "pump is not online"
    exit 2
fi

# stop pump, and pump's state should be paused
binlogctl -pd-urls 127.0.0.1:2379 -cmd pause-pump -node-id pump1:8215
sleep 3
binlogctl -pd-urls 127.0.0.1:2379 -cmd pumps > $statusLog 2>&1
cat $statusLog
count=`grep -c "paused" $statusLog`
if [ $count -ne 1 ]; then
    echo "pump is not paused"
    exit 2
fi

# offline pump, and pump's status should be offline
run_pump &
sleep 3
binlogctl -pd-urls 127.0.0.1:2379 -cmd offline-pump -node-id pump1:8215
sleep 10
binlogctl -pd-urls 127.0.0.1:2379 -cmd pumps > $statusLog 2>&1
cat $statusLog
count=`grep -c "offline" $statusLog`
if [ $count -ne 1 ]; then
    echo "pump is not offline"
    exit 2
fi

# stop drainer, and drainer's state should be paused
binlogctl -pd-urls 127.0.0.1:2379 -cmd pause-drainer -node-id $drainerNodeID
sleep 3
binlogctl -pd-urls 127.0.0.1:2379 -cmd drainers > $statusLog 2>&1
cat $statusLog
count=`grep -c "paused" $statusLog`
if [ $count -ne 1 ]; then
    echo "drainer is not paused"
    exit 2
fi

# offline drainer, and drainer's state should be offline
run_drainer &
sleep 3
binlogctl -pd-urls 127.0.0.1:2379 -cmd offline-drainer -node-id $drainerNodeID
sleep 10
binlogctl -pd-urls 127.0.0.1:2379 -cmd drainers > $statusLog 2>&1
cat $statusLog
count=`grep -c "offline" $statusLog`
if [ $count -ne 1 ]; then
    echo "drainer is not offline"
    exit 2
fi

# update drainer's state to online, and then run pump, pump will notify drainer failed, pump's sttaus will be paused
binlogctl -pd-urls 127.0.0.1:2379 -cmd update-drainer -node-id $drainerNodeID -state online
run_pump &
sleep 3

binlogctl -pd-urls 127.0.0.1:2379 -cmd pumps > $statusLog 2>&1
cat $statusLog
count=`grep -c "paused" $statusLog`
if [ $count -ne 1 ]; then
    echo "pump is not paused"
    exit 2
fi

# clean up
binlogctl -pd-urls 127.0.0.1:2379 -cmd update-drainer -node-id $drainerNodeID -state paused
run_pump &
rm $statusLog || true
