#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer &

run_sql 'DROP DATABASE IF EXISTS update_bit1;'
run_sql 'CREATE DATABASE update_bit1;'
run_sql 'CREATE TABLE update_bit1.t(a BIT(1) NOT NULL);'
run_sql 'INSERT INTO update_bit1.t VALUES (0x01);'

sleep 3

down_run_sql 'SELECT HEX(a) FROM update_bit1.t;'
check_contains 'HEX(a): 1'

# Verify fix for TOOL-1346.

run_sql 'UPDATE update_bit1.t SET a = 0x00;'

sleep 3

down_run_sql 'SELECT HEX(a) FROM update_bit1.t;'
check_contains 'HEX(a): 0'

killall drainer
