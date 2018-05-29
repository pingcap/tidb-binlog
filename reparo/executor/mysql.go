package executor

import (
	"database/sql"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	"github.com/pingcap/tidb-binlog/reparo/common"
	"github.com/pingcap/tidb-binlog/reparo/metrics"
)

type mysqlExecutor struct {
	db *sql.DB
}

func newMysqlExecutor(cfg *common.DBConfig) (Executor, error) {
	db, err := pkgsql.OpenDB("mysql", cfg.Host, cfg.Port, cfg.User, cfg.Password)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &mysqlExecutor{db: db}, nil
}

func (m *mysqlExecutor) Execute(sqls []string, args [][]interface{}, isDDL bool) error {
	if len(sqls) == 0 {
		return nil
	}
	begin := time.Now()
	err := pkgsql.ExecuteSQLs(m.db, sqls, args, isDDL)
	if err != nil {
		return errors.Trace(err)
	}
	cost := time.Since(begin).Seconds()
	if cost > 1 {
		log.Warnf("[reparo] execute sql takes %f seconds, is_ddl %v, length of sqls %d", cost, isDDL, len(sqls))
	} else {
		log.Debugf("[reparo] execute sql takes %f seconds, is_ddl %v, length of sqls %d", cost, isDDL, len(sqls))
	}
	metrics.TxnHistogram.Observe(cost)
	metrics.ExecuteTotalCounter.Add(float64(len(sqls)))

	return nil
}

func (m *mysqlExecutor) Close() error {
	return m.db.Close()
}
