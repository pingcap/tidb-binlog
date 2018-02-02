package executor

// execute sql to mysql/tidb

// TODO

import (
	"database/sql"
)

type mysqlExecutor struct {
	db *sql.DB
}

func newMysqlExecutor() Executor {
	return nil
}

func (m *mysqlExecutor) Execute(sqls []string, args [][]interface{}) error {

	return nil
}

func (m *mysqlExecutor) Close() error {
	return m.db.Close()
}
