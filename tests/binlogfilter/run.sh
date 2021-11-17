#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer &

tables=["do_not_drop_database1.do_not_truncate_table1", "do_not_add_col_database1.do_not_add_col_table1", "do_not_delete_database1.do_not_delete_table1", "test.do_not_filter"]

function clean_up() {
  run_sql "DROP DATABASE IF EXISTS do_not_drop_database1;"
  run_sql "DROP DATABASE IF EXISTS do_not_add_col_database1;"
  run_sql "DROP DATABASE IF EXISTS do_not_delete_database1;"
  run_sql "DROP TABLE IF EXISTS test.do_not_filter;"
}

function run_sql_over_tables() {
  for table in ${tables[@]}; do
    sql=$(echo $1 | sed "s/table-placeholder/$table/g")
    run_sql $sql
	done
}

clean_up

run_sql "CREATE DATABASE do_not_drop_database1;"
run_sql "CREATE DATABASE do_not_add_col_database1;"
run_sql "CREATE DATABASE do_not_delete_database1;"
run_sql "CREATE DATABASE IF NOT EXISTS test;"

run_sql_over_tables "CREATE TABLE table-placeholder(id int);"
run_sql_over_tables "INSERT INTO table-placeholder(id) VALUES (1);"

# check truncate table DDL
run_sql_over_tables "TRUNCATE TABLE table-placeholder;"
sleep 3
down_run_sql "SELECT count(*) FROM do_not_drop_database1.do_not_truncate_table1"
check_contains "0"
down_run_sql "SELECT count(*) FROM do_not_add_col_database1.do_not_add_col_table1"
check_contains "1"
down_run_sql "SELECT count(*) FROM do_not_delete_database1.do_not_delete_table1"
check_contains "1"
down_run_sql "SELECT count(*) FROM test.do_not_filter"
check_contains "1"

# check add column aaa sql pattern
run_sql_over_tables "ALTER TABLE table-placeholder ADD COLUMN aaa int;"
sleep 3
down_run_sql "SHOW COLUMNS FROM do_not_drop_database1.do_not_truncate_table1"
check_not_contains "aaa"
down_run_sql "SHOW COLUMNS FROM do_not_add_col_database1.do_not_add_col_table1"
check_not_contains "aaa"
down_run_sql "SHOW COLUMNS FROM FROM do_not_delete_database1.do_not_delete_table1"
check_contains "aaa"
down_run_sql "SHOW COLUMNS FROM FROM test.do_not_filter"
check_contains "aaa"

# check delete DML
run_sql_over_tables "INSERT INTO table-placeholder(id) VALUES(2);"
run_sql_over_tables "DELETE FROM table-placeholder(id) WHERE id=2;"
sleep 3
down_run_sql "SELECT count(*) FROM do_not_drop_database1.do_not_truncate_table1"
check_contains "1"
down_run_sql "SELECT count(*) FROM do_not_add_col_database1.do_not_add_col_table1"
check_contains "1"
down_run_sql "SELECT count(*) FROM do_not_delete_database1.do_not_delete_table1"
check_contains "2"
down_run_sql "SELECT count(*) FROM test.do_not_filter"
check_contains "1"

clean_up

killall drainer
