#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer &

killall drainer
