#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer &


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

sleep 5
# check
down_run_sql "SHOW DATABASES;"
check_contains "Database: do_start1"
check_contains "Database: do_name"
check_not_contains "Database: do_not_start1"
check_not_contains "Database: do_not_name"

down_run_sql "SHOW TABLES IN test;"
check_contains "Tables_in_test: do_start1"
check_contains "Tables_in_test: do_name"
check_not_contains "Tables_in_test: do_not_start1"
check_not_contains "Tables_in_test: do_not_name"

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
