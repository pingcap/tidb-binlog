#!/bin/sh

set -e

# use latest ts as initial-commit-ts, so we can skip binlog by previous test case
ms=$(date +'%s')
ts=$(($ms*1000<<18))

cd "$(dirname "$0")"

args="-initial-commit-ts=$ts"

kafka_addr=${KAFKA_ADDRS-127.0.0.1:9092}

args="$args -kafka-addrs=$kafka_addr"
run_drainer "$args" &

go build -o out

./out -offset=-1 -topic=binlog_test_topic -kafkaAddr=$kafka_addr -> ${OUT_DIR-/tmp}/$TEST_NAME.out 2>&1

killall drainer
