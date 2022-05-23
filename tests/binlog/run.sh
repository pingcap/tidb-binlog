#!/bin/sh

set -e

cd "$(dirname "$0")"

function revert_timezone() {
    run_sql "SET @@global.time_zone = 'SYSTEM';"
    down_run_sql "SET @@global.time_zone = 'SYSTEM';"
}

# set timezone to others before drainer starts
trap revert_timezone EXIT
run_sql "SET @@global.time_zone = 'Asia/Tokyo';"
down_run_sql "SET @@global.time_zone = 'EST';"

run_drainer &

GO111MODULE=on go build -o out

./out -config ./config.toml > ${OUT_DIR-/tmp}/$TEST_NAME.out 2>&1

killall drainer
