package executor

import (
	"database/sql"

	"github.com/juju/errors"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
)

type mysqlExecutor struct {
	db *sql.DB
}

func newMysql(cfg *DBConfig) (Executor, error) {
	db, err := pkgsql.OpenDB("mysql", cfg.Host, cfg.Port, cfg.User, cfg.Password)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &mysqlExecutor{
		db: db,
	}, nil
}

func (m *mysqlExecutor) Execute(sqls []string, args [][]interface{}, commitTSs []int64, isDDL bool) error {
	return pkgsql.ExecuteSQLs(m.db, sqls, args, isDDL)
}

func (m *mysqlExecutor) ShowPosition() (int64, error) {
	return pkgsql.GetSlavePosition(m.db)
}

func (m *mysqlExecutor) Close() error {
	return m.db.Close()
}
