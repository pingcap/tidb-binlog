package executor

import (
	"database/sql"

	"github.com/juju/errors"
	"github.com/ngaut/log"
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
	err := pkgsql.ExecuteSQLs(m.db, sqls, args, isDDL)
	if err == nil {
		log.Infof("execute success, sqls: %v, args: %v, commitTSs: %v", sqls, args, commitTSs)
	} else {
		log.Infof("execute failed, sqls: %v, args: %v, commitTSs: %v", sqls, args, commitTSs)
	}

	return err
}

func (m *mysqlExecutor) Close() error {
	return m.db.Close()
}
