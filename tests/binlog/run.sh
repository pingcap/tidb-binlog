#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer &

GO111MODULE=on go build -o out

echo "execute out"
./out -config ./config.toml

echo "execute out end"
killall drainer
