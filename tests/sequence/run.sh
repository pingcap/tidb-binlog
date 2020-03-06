#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer &

sleep 3

run_sql 'CREATE DATABASE seq;'
run_sql 'CREATE SEQUENCE seq.sequence_name;'
run_sql 'CREATE TABLE seq.table_name (id INT DEFAULT NEXT VALUE FOR seq.sequence_name, val int);'

run_sql 'INSERT INTO seq.table_name(val) values(10);'

sleep 3

down_run_sql 'SELECT count(*), sum(id), sum(val) FROM seq.table_name;'
check_contains 'count(*): 1'
check_contains 'sum(id): 1'
check_contains 'sum(val): 10'


run_sql 'DROP TABLE seq.table_name;'
run_sql 'DROP SEQUENCE seq.sequence_name;'


killall drainer
