#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer &

sleep 3

run_sql 'CREATE DATABASE placement_test;'
run_sql 'CREATE PLACEMENT POLICY x1 FOLLOWERS=4'
run_sql 'CREATE TABLE placement_test.t1 (a INT NOT NULL PRIMARY KEY, b VARCHAR(100)) PLACEMENT POLICY=x1;'
run_sql "INSERT INTO placement_test.t1 (a,b) VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d');"
run_sql "INSERT INTO placement_test.t1 (a,b) VALUES(5,'e'),(6,'f'),(7,'g'),(8,'h');"

sleep 3

down_run_sql 'SHOW PLACEMENT'
check_not_contains 'Target: POLICY x1'
check_not_contains 'Target: TABLE placement_test.t1'
down_run_sql 'SELECT a, b FROM placement_test.t1 order by a'
check_contains 'a: 7'
check_contains 'b: g'
check_contains 'a: 8'
check_contains 'b: h'

run_sql 'DROP DATABASE placement_test'

killall drainer
