#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer &

sleep 3

run_sql 'create database partition_test;'
# range partition
run_sql 'create table partition_test.t1( a int ,b varchar(128) ) partition by range (a) (partition p0 values less than (3),partition p1 values less than (7));'
run_sql "insert into partition_test.t1 (a,b) values(1,'a'),(2,'b'),(3,'c'),(4,'d');"
run_sql "alter table partition_test.t1 add partition (partition p2 values less than (10));"
run_sql "insert into partition_test.t1 (a,b) values(5,'e'),(6,'f'),(7,'g'),(8,'h');"

# hash partition
run_sql 'create table partition_test.t2(a int ,b varchar(128) ) partition by hash (a) partitions 2;'
run_sql "insert into partition_test.t2 (a,b) values(1,'a'),(2,'b'),(3,'c'),(4,'d');"
run_sql 'truncate table partition_test.t2;'
run_sql "insert into partition_test.t2 (a,b) values(5,'e'),(6,'f'),(7,'g'),(8,'h');"

sleep 3

down_run_sql 'SELECT a, b FROM partition_test.t1 partition(p2) order by a'
check_contains 'a: 7'
check_contains 'b: g'
check_contains 'a: 8'
check_contains 'b: h'

down_run_sql 'select a, b from partition_test.t2 order by a limit 1'
check_contains 'a: 5'
check_contains 'b: e'

run_sql 'DROP database partition_test'

killall drainer
