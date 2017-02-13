package executor

import (
	"database/sql"

	"github.com/juju/errors"
)

type mysqlExecutor struct {
	db *sql.DB
}

func newMysql(cfg *DBConfig) (Executor, error) {
	db, err := openDB("mysql", cfg.Host, cfg.Port, cfg.User, cfg.Password)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &mysqlExecutor{
		db: db,
	}, nil
}

func (m *mysqlExecutor) Execute(sqls []string, args [][]interface{}, commitTSs []int64, isDDL bool) error {
	return executeSQLs(m.db, sqls, args, isDDL)
}

func (m *mysqlExecutor) Close() error {
	return m.db.Close()
}
