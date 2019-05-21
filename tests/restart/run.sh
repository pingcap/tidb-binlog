#!/bin/sh

#set -e

cd "$(dirname "$0")"

OUT_DIR=/tmp/tidb_binlog_test
STATUS_LOG="${OUT_DIR}/status.log"

# run drainer, and drainer's status should be online
# use latest ts as initial-commit-ts, so we can skip binlog by previous test case
ms=$(date +'%s')
ts=$(($ms*1000<<18))
args="-initial-commit-ts=$ts"
down_run_sql "DROP DATABASE IF EXISTS tidb_binlog"
rm -rf /tmp/tidb_binlog_test/data.drainer

echo "Starting a new drainer"
run_drainer "$args" &
sleep 5

echo "Starting a new pump"
run_pump 8251 &
sleep 5

echo "Start inserting test data"
./insert_data &
sleep 5

echo "Restarting pumps one by one"
run_pump 8250
run_pump 8251
sleep .5
run_pump 8251
run_pump 8250

echo "Verifying TiDB is alive after restarting pumps ..."
mysql -uroot -h127.0.0.1 -P4000 --default-character-set utf8 -e 'select * from mysql.tidb;'
if [ $? -ne 0 ]; then
    echo "TiDB is not alive!"
    exit 1
fi

echo "Stop inserting test data"
killall insert_data || true
sleep 2

echo "after kill insert, check data"
i=0
while ! check_data ./sync_diff_inspector.toml; do
    i=$((i+1))
    if [ "$i" -gt 20 ]; then
        echo 'data is not equal'
        exit 1
    fi
    sleep 2
done

echo "data is equal"

# offline a pump
binlogctl -pd-urls 127.0.0.1:2379 -cmd offline-pump -node-id pump:8251
sleep 1
check_status pumps "pump:8251" offline
