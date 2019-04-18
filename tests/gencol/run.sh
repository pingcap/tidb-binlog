#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer &

run_sql 'DROP TABLE IF EXISTS gencol.gct;'
run_sql 'DROP DATABASE IF EXISTS gencol;'
run_sql 'CREATE DATABASE gencol;'
run_sql 'CREATE TABLE gencol.gct(a INT PRIMARY KEY, b INT GENERATED ALWAYS AS (a*a) VIRTUAL, c INT);'

# Verify INSERT statements works with generated columns...

run_sql 'INSERT INTO gencol.gct (a, c) VALUES (1, 10), (2, 12), (3, 32);'

sleep 3

down_run_sql 'SELECT count(*), sum(a), sum(b), sum(c) FROM gencol.gct;'
check_contains 'count(*): 3'
check_contains 'sum(a): 6'
check_contains 'sum(b): 14'
check_contains 'sum(c): 54'

# Verify UPDATE statements works with generated columns...

run_sql 'UPDATE gencol.gct SET a = 7, c = 8 WHERE b = 1;'

sleep 3

down_run_sql 'SELECT count(*), sum(a), sum(b), sum(c) FROM gencol.gct;'
check_contains 'count(*): 3'
check_contains 'sum(a): 12'
check_contains 'sum(b): 62'
check_contains 'sum(c): 52'

run_sql 'UPDATE gencol.gct SET c = b WHERE a = 7;'

sleep 3

down_run_sql 'SELECT count(*), sum(a), sum(b), sum(c) FROM gencol.gct;'
check_contains 'count(*): 3'
check_contains 'sum(a): 12'
check_contains 'sum(b): 62'
check_contains 'sum(c): 93'

# Verify DELETE statements works with generated columns...

run_sql 'DELETE FROM gencol.gct WHERE b = 9;'

sleep 3

down_run_sql 'SELECT count(*), sum(a), sum(b), sum(c) FROM gencol.gct;'
check_contains 'count(*): 2'
check_contains 'sum(a): 9'
check_contains 'sum(b): 53'
check_contains 'sum(c): 61'

killall drainer
