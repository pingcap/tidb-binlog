#!/bin/sh

set -e

cd "$(dirname "$0")"

# use latest ts as initial-commit-ts, so we can skip binlog by previous test case
args="-initial-commit-ts=-1"
down_run_sql "DROP DATABASE IF EXISTS tidb_binlog"

rm -rf /tmp/tidb_binlog_test/data.drainer

run_drainer "$args" &

GO111MODULE=on go build -o out

sleep 5

run_sql "CREATE DATABASE IF NOT EXISTS \`reparo-test\`"

./out -config ./config.toml > ${OUT_DIR-/tmp}/$TEST_NAME.out 2>&1

sleep 5

run_reparo &

sleep 15

check_data ./sync_diff_inspector.toml 

# clean up
run_sql "DROP DATABASE IF EXISTS \`reparo-test\`"

killall drainer
