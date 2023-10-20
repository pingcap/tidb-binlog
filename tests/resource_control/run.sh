#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer --compressor gzip &

sleep 3

run_sql 'CREATE DATABASE resource_control_test;'
run_sql 'CREATE RESOURCE GROUP rg1 RU_PER_SEC=10000;'
run_sql 'CREATE RESOURCE GROUP rg2 RU_PER_SEC=5000;'
run_sql 'ALTER RESOURCE GROUP rg1 RU_PER_SEC=10000, BURSTABLE;'
run_sql 'DROP RESOURCE GROUP rg2;'

sleep 3

run_sql 'DROP DATABASE resource_control_test;'

killall drainer
