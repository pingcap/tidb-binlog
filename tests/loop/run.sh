#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer &

sleep 5

run_sql "DROP DATABASE IF EXISTS retl;"
run_sql "DROP TABLE IF EXISTS retl._drainer_repl_mark;"
run_sql "DROP TABLE IF EXISTS test.loop;"

# set up tables.
run_sql "CREATE database retl;"
run_sql "CREATE TABLE If Not Exists retl._drainer_repl_mark(channel_id bigint primary key, val bigint DEFAULT 0, channel_info varchar(64));"


# create a table for test mark status
run_sql "CREATE TABLE test.loop(id int primary key);"
# normal txn should be replicated.
run_sql "INSERT INTO test.loop(id) VALUES (1);"
sleep 5
down_run_sql "SELECT count(*) FROM test.loop;"
check_contains "count(*): 1"

# txn with the mark table and according channel id will not be replicated.
run_sql "BEGIN; INSERT INTO test.loop(id) VALUES (2); REPLACE INTO retl._drainer_repl_mark(channel_id,val) VALUES(1,10); commit;"
sleep 5
down_run_sql "SELECT count(*) FROM test.loop;"
check_contains "count(*): 1"

# clean up
run_sql "DROP DATABASE IF EXISTS retl;";
run_sql "DROP TABLE IF EXISTS retl._drainer_repl_mark;"
run_sql "DROP TABLE IF EXISTS test.loop;"

killall drainer
