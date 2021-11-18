#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer &

sleep 3

run_sql "CREATE DATABASE attr;"
run_sql "CREATE TABLE attr.attributes_t1 (id INT PRIMARY KEY, name VARCHAR(50));"

run_sql "ALTER TABLE attr.attributes_t1 ATTRIBUTES='merge_option=deny';"
run_sql "INSERT INTO attr.attributes_t1 (id, name) VALUES (1, 'test1');"

run_sql "CREATE TABLE attr.attributes_t2 (id INT PRIMARY KEY, name VARCHAR(50)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10000), PARTITION p1 VALUES LESS THAN (MAXVALUE));"
run_sql "ALTER TABLE attr.attributes_t2 ATTRIBUTES='merge_option=deny';"
run_sql "ALTER TABLE attr.attributes_t2 PARTITION p0 ATTRIBUTES='merge_option=allow';"
run_sql "INSERT INTO attr.attributes_t2 (id, name) VALUES (2, 'test2');"

run_sql "DROP TABLE attr.attributes_t1;"
run_sql "RECOVER TABLE attr.attributes_t1;"
run_sql "TRUNCATE TABLE attr.attributes_t1;"
run_sql "FLASHBACK TABLE attr.attributes_t1 TO attributes_t1_back;"
run_sql "RENAME TABLE attr.attributes_t1_back TO attr.attributes_t1_new;"
run_sql "ALTER TABLE attr.attributes_t2 DROP PARTITION p0;"
run_sql "ALTER TABLE attr.attributes_t2 TRUNCATE PARTITION p1;"

sleep 3

down_run_sql "SELECT count(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_SCHEMA='attr';"
check_contains "count(TABLE_NAME): 3"

run_sql "DROP TABLE attr.attributes_t1;"
run_sql "DROP TABLE attr.attributes_t1_new;"
run_sql "DROP TABLE attr.attributes_t2;"

killall drainer
