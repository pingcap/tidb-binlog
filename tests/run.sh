#!/bin/sh

set -eu

OUT_DIR=/tmp/tidb_binlog_test

mkdir -p $OUT_DIR || true
# to the dir of this script
cd "$(dirname "$0")"

pwd=$(pwd)

export PATH=$PATH:$pwd/_utils
export PATH=$PATH:$(dirname $pwd)/bin

generate_tls_keys() {
    # Ref: https://docs.microsoft.com/en-us/azure/application-gateway/self-signed-certificates
    # gRPC only supports P-256 curves, see https://github.com/grpc/grpc/issues/6722
    echo "Generate TLS keys..."
    TT="$OUT_DIR/cert"
    mkdir -p $TT || true

    cat - > "$TT/ipsan.cnf" <<EOF
[dn]
CN = localhost
[req]
distinguished_name = dn
[EXT]
subjectAltName = @alt_names
keyUsage = digitalSignature,keyEncipherment
extendedKeyUsage = clientAuth,serverAuth
[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF
    openssl ecparam -out "$TT/ca.key" -name prime256v1 -genkey
    openssl req -new -batch -sha256 -subj '/CN=binlog' -key "$TT/ca.key" -out "$TT/ca.csr"
    openssl x509 -req -sha256 -days 2 -in "$TT/ca.csr" -signkey "$TT/ca.key" -out "$TT/ca.pem" 2> /dev/null

    for name in tidb pd tikv pump drainer client; do
        openssl ecparam -out "$TT/$name.key" -name prime256v1 -genkey
        openssl req -new -batch -sha256 -subj '/CN=binlog' -key "$TT/$name.key" -out "$TT/$name.csr"
        openssl x509 -req -sha256 -days 1 -extensions EXT -extfile "$TT/ipsan.cnf" -in "$TT/$name.csr" -CA "$TT/ca.pem" -CAkey "$TT/ca.key" -CAcreateserial -out "$TT/$name.pem" 2> /dev/null
    done
}


clean_data() {
    rm -rf $OUT_DIR/* || true
}

stop_services() {
    killall -9 tikv-server || true
    killall -9 pd-server || true
    killall -9 tidb-server || true

    killall -9 pump || true
    killall -9 drainer || true
}

start_upstream_tidb() {
    cat - > "$OUT_DIR/tidb-config.toml" <<EOF
[performance]
txn-total-size-limit = 104857599

[security]
# set the path for certificates. Empty string means disabling secure connectoins.
cluster-ssl-ca = "$OUT_DIR/cert/ca.pem"
cluster-ssl-cert = "$OUT_DIR/cert/tidb.pem"
cluster-ssl-key = "$OUT_DIR/cert/tidb.key"

[experimental]
allow-auto-random = true
EOF

    port=${1-4000}
    sport=${2-10080}
    echo "Starting TiDB at port: $port..."
    tidb-server \
        -P $port \
        -status $sport \
        -config "$OUT_DIR/tidb-config.toml" \
        --store tikv \
        --path 127.0.0.1:2379 \
        --enable-binlog=true \
        --log-file "$OUT_DIR/tidb.log" &

    echo "Verifying TiDB is started..."
    i=0
    while ! mysql -uroot -h127.0.0.1 -P$port --default-character-set utf8 -e 'select * from mysql.tidb;'; do
        i=$((i+1))
        if [ "$i" -gt 40 ]; then
            echo 'Failed to start TiDB'
            exit 1
        fi
        sleep 3
    done

}

start_services() {
    stop_services
    clean_data
    generate_tls_keys

    cat - > "$OUT_DIR/pd-config.toml" <<EOF
[security]
# set the path for certificates. Empty string means disabling secure connectoins.
cacert-path = "$OUT_DIR/cert/ca.pem"
cert-path = "$OUT_DIR/cert/pd.pem"
key-path = "$OUT_DIR/cert/pd.key"
EOF

    echo "Starting PD..."
    pd-server \
        --client-urls https://127.0.0.1:2379 \
        --peer-urls https://127.0.0.1:2380 \
        --advertise-client-urls https://127.0.0.1:2379 \
        --advertise-peer-urls https://127.0.0.1:2380 \
        -config "$OUT_DIR/pd-config.toml" \
        --log-file "$OUT_DIR/pd.log" \
        --data-dir "$OUT_DIR/pd" &

    # wait until PD is online...
	# use wget, on CI curl's version is too old: curl: (58) unable to load client key: -8178 (SEC_ERROR_BAD_KEY)))))
    while ! wget -q -O - \
    --ca-certificate="$OUT_DIR/cert/ca.pem" \
    --certificate="$OUT_DIR/cert/client.pem" \
    --private-key="$OUT_DIR/cert/client.key" \
    https://127.0.0.1:2379/pd/api/v1/version; do
        sleep 1
    done

    echo "Starting downstream PD..."
    pd-server \
        --client-urls http://127.0.0.1:2381 \
        --peer-urls http://127.0.0.1:2382 \
        --log-file "$OUT_DIR/down_pd.log" \
        --data-dir "$OUT_DIR/down_pd" &

    # wait until downstream PD is online...
    while ! curl -o /dev/null -sf http://127.0.0.1:2381/pd/api/v1/version; do
        sleep 1
    done

    # Tries to limit the max number of open files under the system limit
    cat - > "$OUT_DIR/tikv-config.toml" <<EOF
[rocksdb]
max-open-files = 4096
[raftdb]
max-open-files = 4096
[raftstore]
# true (default value) for high reliability, this can prevent data loss when power failure.
sync-log = false

[security]
# set the path for certificates. Empty string means disabling secure connectoins.
ca-path = "$OUT_DIR/cert/ca.pem"
cert-path = "$OUT_DIR/cert/tikv.pem"
key-path = "$OUT_DIR/cert/tikv.key"
EOF

    echo "Starting TiKV..."
    tikv-server \
        --pd 127.0.0.1:2379 \
        --advertise-addr 127.0.0.1:20160 \
        -A 127.0.0.1:20160 \
        --log-file "$OUT_DIR/tikv.log" \
        -C "$OUT_DIR/tikv-config.toml" \
        -s "$OUT_DIR/tikv" &
    sleep 1

    # Tries to limit the max number of open files under the system limit
    cat - > "$OUT_DIR/down-tikv-config.toml" <<EOF
[rocksdb]
max-open-files = 4096
[raftdb]
max-open-files = 4096
[raftstore]
# true (default value) for high reliability, this can prevent data loss when power failure.
sync-log = false
EOF

    echo "Starting downstream TiKV..."
    tikv-server \
        --pd 127.0.0.1:2381 \
        -A 127.0.0.1:20161 \
        --log-file "$OUT_DIR/down_tikv.log" \
        -C "$OUT_DIR/down-tikv-config.toml" \
        -s "$OUT_DIR/down_tikv" &
    sleep 1


    echo "Starting Pump..."
    run_pump &

    sleep 5

    start_upstream_tidb 4000 10080
    start_upstream_tidb 4001 10081


    cat - > "$OUT_DIR/down-tidb-config.toml" <<EOF
[experimental]
allow-auto-random = true
EOF

    echo "Starting Downstream TiDB..."
    tidb-server \
        -P 3306 \
        -config "$OUT_DIR/down-tidb-config.toml" \
        --store tikv \
        --path 127.0.0.1:2381 \
        --status=20080 \
        --log-file "$OUT_DIR/down_tidb.log" &

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
    run_drainer -L debug &
    echo "Verifying drainer is started..."
    while ! wget -q -O - \
    --ca-certificate="$OUT_DIR/cert/ca.pem" \
    --certificate="$OUT_DIR/cert/client.pem" \
    --private-key="$OUT_DIR/cert/client.key" \
    https://127.0.0.1:8249/status; do
        sleep 1
    done
}


trap stop_services EXIT
start_services

if [ "${1-}" = '--debug' ]; then
    echo 'You may now debug from another terminal. Press [ENTER] to continue.'
    read line
fi

run_case() {
    local case=$1
    local script=$2
    echo "Running test $script..."
    PATH="$pwd/../bin:$pwd/_utils:$PATH" \
    OUT_DIR=$OUT_DIR \
    TEST_NAME=$case \
    sh "$script"
}

# List the case names to run, eg. ("binlog" "kafka")
do_cases=("binlog")

if [ ${#do_cases[@]} -eq 0 ]; then
    for script in ./*/run.sh; do
        test_name="$(basename "$(dirname "$script")")"
        run_case $test_name $script
    done
else
    for case in "${do_cases[@]}"; do
        script="./$case/run.sh"
        run_case $case $script
    done
fi

# with color
echo "\033[0;36m<<< Run all test success >>>\033[0m"
