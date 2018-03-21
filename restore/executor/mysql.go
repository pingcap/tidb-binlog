package executor

// execute sql to mysql/tidb

import (
	"database/sql"

	"github.com/juju/errors"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
)

// DBConfig is the DB configuration.
type DBConfig struct {
	Host     string `toml:"host" json:"host"`
	User     string `toml:"user" json:"user"`
	Password string `toml:"password" json:"password"`
	Port     int    `toml:"port" json:"port"`
}

type mysqlExecutor struct {
	db *sql.DB
}

func newMysqlExecutor(cfg *DBConfig) (Executor, error) {
	db, err := pkgsql.OpenDB("mysql", cfg.Host, cfg.Port, cfg.User, cfg.Password)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &mysqlExecutor{db: db}, nil
}

func (m *mysqlExecutor) Execute(sqls []string, args [][]interface{}, isDDL bool) error {
	return errors.Trace(pkgsql.ExecuteSQLs(m.db, sqls, args, isDDL))
}

func (m *mysqlExecutor) Close() error {
	return m.db.Close()
}
