#!/bin/sh

set -eu

OUT_DIR=/tmp/tidb_binlog_test

mkdir $OUT_DIR || true
# to the dir of this script
cd "$(dirname "$0")"

pwd=$(pwd)

export PATH=$PATH:$pwd/_utils
export PATH=$PATH:$(dirname $pwd)/bin


clean_data() {
	rm -rf $OUT_DIR/pd || true
	rm -rf $OUT_DIR/tidb || true
	rm -rf $OUT_DIR/tikv || true
	rm -rf $OUT_DIR/pump || true
	rm -rf $OUT_DIR/data.drainer || true
}

stop_services() {
    killall -9 tikv-server || true
    killall -9 pd-server || true
    killall -9 tidb-server || true

    killall -9 pump || true
    killall -9 drainer || true
}

start_services() {
    stop_services
	clean_data

    echo "Starting PD..."
    run_pd &
    
    # wait until PD is online...
    while ! curl -o /dev/null -sf http://127.0.0.1:2379/pd/api/v1/version; do
        sleep 1
    done

    # Tries to limit the max number of open files under the system limit
    cat - > "$OUT_DIR/tikv-config.toml" <<EOF
[rocksdb]
max-open-files = 4096
[raftdb]
max-open-files = 4096
EOF

    echo "Starting TiKV..."
    run_tikv &
    sleep 1

    echo "Starting Pump..."
	run_pump 8250 &

	sleep 5


    echo "Starting TiDB..."
    run_tidb 4000 tikv &

    echo "Verifying TiDB is started..."
    i=0
    while ! mysql -uroot -h127.0.0.1 -P4000 --default-character-set utf8 -e 'select * from mysql.tidb;'; do
        i=$((i+1))
        if [ "$i" -gt 40 ]; then
            echo 'Failed to start TiDB'
            exit 1
        fi
        sleep 3
    done

    echo "Starting Downstream TiDB..."
    run_tidb 3306 mocktikv &

    echo "Verifying Downstream TiDB is started..."
    i=0
    while ! mysql -uroot -h127.0.0.1 -P3306 --default-character-set utf8 -e 'select * from mysql.tidb;'; do
        i=$((i+1))
        if [ "$i" -gt 10 ]; then
            echo 'Failed to start TiDB'
            exit 1
        fi
        sleep 3
    done

	echo "Starting Drainer..."
	run_drainer &
}

trap stop_services EXIT
start_services

if [ "${1-}" = '--debug' ]; then
    echo 'You may now debug from another terminal. Press [ENTER] to continue.'
    read line
fi

# set to the case name you want to run only for debug
do_case=""

for script in ./*/run.sh; do
	test_name="$(basename "$(dirname "$script")")"
	if [[ $do_case != "" && $test_name != $do_case ]]; then
		continue
	fi

    echo "Running test $script..."
    PATH="$pwd/../bin:$pwd/_utils:$PATH" \
	OUT_DIR=$OUT_DIR \
    TEST_NAME=$test_name \
    sh "$script"
done

echo "<<< Run all test success >>>"
