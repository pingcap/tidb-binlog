package executor

import (
	"database/sql"
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pingcap/errors"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
)

// QueryHistogramVec get the sql query time
var QueryHistogramVec *prometheus.HistogramVec

type mysqlExecutor struct {
	db      *sql.DB
	sqlMode mysql.SQLMode
	*baseError
}

func newMysql(cfg *DBConfig, strSQLMode *string) (Executor, error) {
	db, err := pkgsql.OpenDBWithSQLMode("mysql", cfg.Host, cfg.Port, cfg.User, cfg.Password, strSQLMode)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sqlMode, err := mysql.GetSQLMode(*strSQLMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &mysqlExecutor{
		db:        db,
		sqlMode:   sqlMode,
		baseError: newBaseError(),
	}, nil
}

func (m *mysqlExecutor) Execute(sqls []string, args [][]interface{}, commitTSs []int64, isDDL bool) error {
	if isDDL {
		ddlParser := parser.New()
		ddlParser.SetSQLMode(m.sqlMode)
		stmt, err := ddlParser.ParseOneStmt(sqls[0], "", "")
		if err != nil {
			return errors.Trace(err)
		}

		_, isCreateDatabase := stmt.(*ast.CreateDatabaseStmt)
		if !isCreateDatabase {
			schema := args[0][0]
			useSQL := fmt.Sprintf("use %s;", schema)
			sqls = append([]string{useSQL}, sqls...)
		}
		args = make([][]interface{}, len(sqls))
	}
	return pkgsql.ExecuteSQLsWithHistogram(m.db, sqls, args, isDDL, QueryHistogramVec)
}

func (m *mysqlExecutor) Close() error {
	return m.db.Close()
}
