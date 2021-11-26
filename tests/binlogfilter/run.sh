#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer -L debug &

function clean_up() {
  run_sql "DROP DATABASE IF EXISTS do_not_truncate_database;"
  run_sql "DROP DATABASE IF EXISTS do_not_add_col_database1;"
  run_sql "DROP DATABASE IF EXISTS do_not_delete_database1;"
  run_sql "DROP TABLE IF EXISTS test.do_not_filter;"
}

clean_up

run_sql "CREATE DATABASE do_not_truncate_database;"
run_sql "CREATE DATABASE do_not_add_col_database1;"
run_sql "CREATE DATABASE do_not_delete_database1;"
run_sql "CREATE DATABASE IF NOT EXISTS test;"

run_sql "CREATE TABLE do_not_truncate_database.do_not_truncate_table1(id int);"
run_sql "CREATE TABLE do_not_add_col_database1.do_not_add_col_table1(id int);"
run_sql "CREATE TABLE do_not_delete_database1.do_not_delete_table1(id int);"
run_sql "CREATE TABLE test.do_not_filter(id int);"

run_sql "INSERT INTO do_not_truncate_database.do_not_truncate_table1(id) VALUES (1);"
run_sql "INSERT INTO do_not_add_col_database1.do_not_add_col_table1(id) VALUES (1);"
run_sql "INSERT INTO do_not_delete_database1.do_not_delete_table1(id) VALUES (1);"
run_sql "INSERT INTO test.do_not_filter(id) VALUES (1);"

# check truncate table DDL
run_sql "TRUNCATE TABLE do_not_truncate_database.do_not_truncate_table1;"
run_sql "TRUNCATE TABLE do_not_add_col_database1.do_not_add_col_table1;"
run_sql "TRUNCATE TABLE do_not_delete_database1.do_not_delete_table1;"
run_sql "TRUNCATE TABLE test.do_not_filter;"

sleep 3
down_run_sql "SELECT count(*) FROM do_not_truncate_database.do_not_truncate_table1"
check_contains "count(*): 1"
down_run_sql "SELECT count(*) FROM do_not_add_col_database1.do_not_add_col_table1"
check_contains "count(*): 0"
down_run_sql "SELECT count(*) FROM do_not_delete_database1.do_not_delete_table1"
check_contains "count(*): 0"
down_run_sql "SELECT count(*) FROM test.do_not_filter"
check_contains "count(*): 0"

# check add column aaa sql pattern
run_sql "ALTER TABLE do_not_truncate_database.do_not_truncate_table1 ADD COLUMN aaa int;"
run_sql "ALTER TABLE do_not_add_col_database1.do_not_add_col_table1 ADD COLUMN aaa int;"
run_sql "ALTER TABLE do_not_delete_database1.do_not_delete_table1 ADD COLUMN aaa int;"
run_sql "ALTER TABLE test.do_not_filter ADD COLUMN aaa int;"

sleep 3
down_run_sql "SHOW COLUMNS FROM do_not_truncate_database.do_not_truncate_table1"
check_not_contains "aaa"
down_run_sql "SHOW COLUMNS FROM do_not_add_col_database1.do_not_add_col_table1"
check_not_contains "aaa"
down_run_sql "SHOW COLUMNS FROM do_not_delete_database1.do_not_delete_table1"
check_contains "aaa"
down_run_sql "SHOW COLUMNS FROM test.do_not_filter"
check_contains "aaa"

# check delete DML in several transactions
run_sql "INSERT INTO do_not_truncate_database.do_not_truncate_table1(id) VALUES(2);"
run_sql "INSERT INTO do_not_add_col_database1.do_not_add_col_table1(id) VALUES(2);"
run_sql "INSERT INTO do_not_delete_database1.do_not_delete_table1(id) VALUES(2);"
run_sql "INSERT INTO test.do_not_filter(id) VALUES(2);"

run_sql "DELETE FROM do_not_truncate_database.do_not_truncate_table1 WHERE id=2;"
run_sql "DELETE FROM do_not_add_col_database1.do_not_add_col_table1 WHERE id=2;"
run_sql "DELETE FROM do_not_delete_database1.do_not_delete_table1 WHERE id=2;"
run_sql "DELETE FROM test.do_not_filter WHERE id=2;"

sleep 3
down_run_sql "SELECT count(*) FROM do_not_truncate_database.do_not_truncate_table1"
check_contains "count(*): 2"
down_run_sql "SELECT count(*) FROM do_not_add_col_database1.do_not_add_col_table1"
check_contains "count(*): 0"
down_run_sql "SELECT count(*) FROM do_not_delete_database1.do_not_delete_table1"
check_contains "count(*): 1"
down_run_sql "SELECT count(*) FROM test.do_not_filter"
check_contains "count(*): 0"

clean_up

killall drainer
