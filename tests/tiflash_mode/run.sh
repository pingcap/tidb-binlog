#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer &

sleep 3

run_sql 'CREATE DATABASE tiflash_mode_test;'
run_sql 'CREATE TABLE tiflash_mode_test.t1( a int, b varchar(128));'
run_sql 'ALTER TABLE tiflash_mode_test.t1 SET TIFLASH REPLICA 1;'
run_sql 'ALTER TABLE tiflash_mode_test.t1 SET TIFLASH MODE FAST;'

sleep 3

down_run_sql 'select table_mode from information_schema.tiflash_replica where table_name = t1 and table_schema = tiflash_mode_test'
check_contains 'FAST'


run_sql 'DROP DATABASE cache_test'

killall drainer