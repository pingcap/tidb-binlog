#!/bin/sh


set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_binlog_test
STATUS_LOG="${OUT_DIR}/status.log"

# use latest ts as initial-commit-ts, so we can skip binlog by previous test case
args="-initial-commit-ts=-1"
down_run_sql "DROP DATABASE IF EXISTS tidb_binlog"
rm -rf /tmp/tidb_binlog_test/data.drainer


# run drainer, and drainer's status should be online
run_drainer "$args" &

sleep 2
echo "check drainer's status, should be online"
check_status drainers online

# TODO improve it, let binlogctl output formatted data
drainerNodeID=`cat $STATUS_LOG | sed 's/.*NodeID: \(.*:[0-9]*\), Addr:.*/\1/g'`
echo "get drainer node id: $drainerNodeID"

pumpNodeID="pump:8250"

# pump's state should be online
echo "check pump's status, should be online"
check_status pumps $pumpNodeID online

# stop pump, and pump's state should be paused
binlogctl -pd-urls 127.0.0.1:2379 -cmd pause-pump -node-id $pumpNodeID

echo "check pump's status, should be paused"
check_status pumps $pumpNodeID paused

# offline pump, and pump's status should be offline
run_pump &
sleep 3
binlogctl -pd-urls 127.0.0.1:2379 -cmd offline-pump -node-id $pumpNodeID

echo "check pump's status, should be offline"
check_status pumps $pumpNodeID offline

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

# update drainer's state to online, and then run pump, pump will notify drainer failed, pump's status will be paused
binlogctl -pd-urls 127.0.0.1:2379 -cmd update-drainer -node-id $drainerNodeID -state online
run_pump &

echo "check pump's status, should be offline"
check_status pumps $pumpNodeID offline

# clean up
binlogctl -pd-urls 127.0.0.1:2379 -cmd update-drainer -node-id $drainerNodeID -state paused
run_pump &
rm $STATUS_LOG || true
