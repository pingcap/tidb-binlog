#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer &

# according to the config, db name or table start with `do_start` or exact equals `do_name` will be synced to downstream
# we use the name start with `do_no_start` and `do_not_name` to make sure other db or table will not be synced to downstream 

run_sql "DROP DATABASE IF EXISTS do_start1;"
run_sql "DROP DATABASE IF EXISTS do_name;"
run_sql "DROP DATABASE IF EXISTS do_not_start1;"
run_sql "DROP DATABASE IF EXISTS do_not_name;"

run_sql "DROP TABLE IF EXISTS test.do_start1;"
run_sql "DROP TABLE IF EXISTS test.do_name;"
run_sql "DROP TABLE IF EXISTS test.do_not_start1;"
run_sql "DROP TABLE IF EXISTS test.do_not_name;"


run_sql "CREATE DATABASE do_start1;"
run_sql "CREATE DATABASE do_name;"
run_sql "CREATE DATABASE do_not_start1;"
run_sql "CREATE DATABASE do_not_name;"

run_sql "CREATE TABLE test.do_start1(id int);"
run_sql "CREATE TABLE test.do_name(id int);"
run_sql "CREATE TABLE test.do_not_start1(id int);"
run_sql "CREATE TABLE test.do_not_name(id int);"

run_sql "INSERT INTO test.do_start1(id) VALUES (1);"
run_sql "INSERT INTO test.do_name(id) VALUES (1);"
run_sql "INSERT INTO test.do_not_start1(id) VALUES (1);"
run_sql "INSERT INTO test.do_not_name(id) VALUES (1);"


sleep 5
# check Database exists or not
down_run_sql "SHOW DATABASES;"
check_contains "Database: do_start1"
check_contains "Database: do_name"
check_not_contains "Database: do_not_start1"
check_not_contains "Database: do_not_name"

# check table exists or not
down_run_sql "SHOW TABLES IN test;"
check_contains "Tables_in_test: do_start1"
check_contains "Tables_in_test: do_name"
check_not_contains "Tables_in_test: do_not_start1"
check_not_contains "Tables_in_test: do_not_name"

# check DML
down_run_sql "SELECT count(*) FROM test.do_start1;"
check_contains "count(*): 1"
down_run_sql "SELECT count(*) FROM test.do_name;"
check_contains "count(*): 1"

# clean up
run_sql "DROP DATABASE IF EXISTS do_start1;"
run_sql "DROP DATABASE IF EXISTS do_name;"
run_sql "DROP DATABASE IF EXISTS do_not_start1;"
run_sql "DROP DATABASE IF EXISTS do_not_name;"

run_sql "DROP TABLE IF EXISTS test.do_start1;"
run_sql "DROP TABLE IF EXISTS test.do_name;"
run_sql "DROP TABLE IF EXISTS test.do_not_start1;"
run_sql "DROP TABLE IF EXISTS test.do_not_name;"


killall drainer
