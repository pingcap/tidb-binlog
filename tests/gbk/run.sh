#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer &

sleep 3

run_sql 'CREATE DATABASE gbk_test DEFAULT CHARACTER SET gbk COLLATE gbk_bin;'
run_sql 'CREATE TABLE gbk_test.t1 (a INT NOT NULL PRIMARY KEY, b VARCHAR(100) charset gbk collate gbk_bin);'
run_sql "INSERT INTO gbk_test.t1 (a,b) VALUES(1,'钛'),(2,'滴'),(3,'比'),(4,'滨');"
run_sql "INSERT INTO gbk_test.t1 (a,b) VALUES(5,'罗'),(6,'格'),(7,'结'),(8,'束');"

sleep 3

down_run_sql 'SELECT a, b FROM gbk_test.t1 order by a'
check_contains 'a: 7'
check_contains 'b: 结'
check_contains 'a: 8'
check_contains 'b: 束'

run_sql 'DROP DATABASE gbk_test'

killall drainer
