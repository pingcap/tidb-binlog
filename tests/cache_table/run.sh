#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer --compressor gzip &

sleep 3

run_sql 'CREATE DATABASE cache_test;'
run_sql 'CREATE TABLE cache_test.t1( a int, b varchar(128));'
run_sql "INSERT INTO cache_test.t1 (a,b) VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d');"
run_sql "ALTER TABLE cache_test.t1 CACHE;"
run_sql "INSERT INTO cache_test.t1 (a,b) VALUES(5,'e'),(6,'f'),(7,'g'),(8,'h');"
run_sql "ALTER TABLE cache_test.t1 NOCACHE"

sleep 3

down_run_sql 'SELECT a, b FROM cache_test.t1 order by a'
check_contains 'a: 7'
check_contains 'b: g'
check_contains 'a: 8'
check_contains 'b: h'

run_sql 'DROP DATABASE cache_test'

killall drainer
