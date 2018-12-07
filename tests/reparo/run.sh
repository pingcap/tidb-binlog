#!/bin/sh

set -e

cd "$(dirname "$0")"

mkdir data.drainer
run_drainer &

# according to the config, db name or table start with `do_start` or exact equals `do_name` will be synced to downstream
# we use the name start with `do_no_start` and `do_not_name` to make sure other db or table will not be synced to downstream 

run_sql "DROP DATABASE IF EXISTS reparo_test;"
run_sql "CREATE DATABASE reparo_test"
run_sql "CREATE TABLE reparo_test.test(id int, name varchar(10), PRIMARY KEY(id))"

run_sql "INSERT INTO reparo_test.test VALUES(1, 'a'), (2, 'b')"
run_sql "INSERT INTO reparo_test.test VALUES(3, 'c'), (4, 'd')"
run_sql "UPDATE reparo_test.test SET name = 'bb' where id = 2"
run_sql "DELETE FROM reparo_test.test WHERE name = 'bb'"
run_sql "INSERT INTO reparo_test.test VALUES(5, 'e')"

sleep 5

run_reparo &

sleep 5

down_run_sql "SELECT count(*) FROM reparo_test.test"
check_contains "count(*): 4"

# clean up
run_sql "DROP DATABASE IF EXISTS reparo_test"

killall drainer
