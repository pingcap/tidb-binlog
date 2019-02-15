package executor

import (
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pingcap/errors"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
)

// QueryHistogram get the sql query time
var QueryHistogramVec *prometheus.HistogramVec

type mysqlExecutor struct {
	db *sql.DB
	*baseError
}

func newMysql(cfg *DBConfig) (Executor, error) {
	db, err := pkgsql.OpenDB("mysql", cfg.Host, cfg.Port, cfg.User, cfg.Password)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &mysqlExecutor{
		db:        db,
		baseError: newBaseError(),
	}, nil
}

func (m *mysqlExecutor) Execute(sqls []string, args [][]interface{}, commitTSs []int64, isDDL bool) error {
	return pkgsql.ExecuteSQLsWithHistogram(m.db, sqls, args, isDDL, QueryHistogramVec)
}

func (m *mysqlExecutor) Close() error {
	return m.db.Close()
}
