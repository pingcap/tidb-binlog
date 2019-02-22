#!/bin/sh

set -e

cd "$(dirname "$0")"

run_drainer &

run_sql "DROP DATABASE IF EXISTS \`reparo_test\`;"
run_sql "CREATE DATABASE \`reparo_test\`"
run_sql "CREATE TABLE \`reparo_test\`.\`test\`(\`id\` int, \`name\` varchar(10), \`all\` varchar(10), PRIMARY KEY(\`id\`))"

run_sql "INSERT INTO \`reparo_test\`.\`test\` VALUES(1, 'a', 'a'), (2, 'b', 'b')"
run_sql "INSERT INTO \`reparo_test\`.\`test\` VALUES(3, 'c', 'c'), (4, 'd', 'c')"
run_sql "UPDATE \`reparo_test\`.\`test\` SET \`name\` = 'bb' where \`id\` = 2"
run_sql "DELETE FROM \`reparo_test\`.\`test\` WHERE \`name\` = 'bb'"
run_sql "INSERT INTO \`reparo_test\`.\`test\` VALUES(5, 'e', 'e')"

sleep 5

run_reparo &

sleep 5

down_run_sql "SELECT count(*) FROM \`reparo_test\`.\`test\`"
check_contains "count(*): 4"

# clean up
run_sql "DROP DATABASE IF EXISTS \`reparo_test\`"

killall drainer
