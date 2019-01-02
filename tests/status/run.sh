#!/bin/sh

 set -e

 cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_binlog_test
STATUS_LOG="${OUT_DIR}/status.log"

# run drainer, and drainer's status should be online
run_drainer &
echo "check drainer's status, should be online"
check_status drainers online

drainerNodeID=`cat $STATUS_LOG | sed 's/.*NodeID:\([a-zA-Z0-9\-]*:[0-9]*\) .*/\1/g'`

# pump's state should be online
echo "check pump's status, should be online"
check_status pumps online

max_commit_ts_1=`cat $STATUS_LOG | sed 's/.*MaxCommitTS:\([0-9]*\) .*/\1/g'`

check_status pumps online

max_commit_ts_2=`cat $STATUS_LOG | sed 's/.*MaxCommitTS:\([0-9]*\) .*/\1/g'`


# stop pump, and pump's state should be paused
binlogctl -pd-urls 127.0.0.1:2379 -cmd pause-pump -node-id pump1:8215

echo "check pump's status, should be paused"
check_status pumps paused

# offline pump, and pump's status should be offline
run_pump &
sleep 3
binlogctl -pd-urls 127.0.0.1:2379 -cmd offline-pump -node-id pump1:8215

echo "check pump's status, should be offline"
check_status pumps offline


# stop drainer, and drainer's state should be paused
binlogctl -pd-urls 127.0.0.1:2379 -cmd pause-drainer -node-id $drainerNodeID

echo "check drainer's status, should be paused"
check_status drainers paused

# offline drainer, and drainer's state should be offline
run_drainer &
sleep 3
binlogctl -pd-urls 127.0.0.1:2379 -cmd offline-drainer -node-id $drainerNodeID

echo "check drainer's status, should be offline"
check_status drainers offline

# update drainer's state to online, and then run pump, pump will notify drainer failed, pump's sttaus will be paused
binlogctl -pd-urls 127.0.0.1:2379 -cmd update-drainer -node-id $drainerNodeID -state online
run_pump &

echo "check pump's status, should be paused"
check_status pumps paused

# clean up
binlogctl -pd-urls 127.0.0.1:2379 -cmd update-drainer -node-id $drainerNodeID -state paused
run_pump &
rm $STATUS_LOG || true
