#!/bin/sh

set -eu

OUT_DIR=/tmp/tidb_binlog_test

# to the dir of this script
cd "$(dirname "$0")"

pwd=$(pwd)

mkdir $OUT_DIR || true

for script in ./*/run.sh; do
    echo "Running test $script..."
    PATH="$pwd/../bin:$pwd/_utils:$PATH" \
	OUT_DIR=$OUT_DIR \
    TEST_NAME="$(basename "$(dirname "$script")")" \
    sh "$script"
done
