#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer &

GO111MODULE=on go build -o out

./out -config ./config.toml

killall drainer
