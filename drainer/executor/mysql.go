// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pingcap/errors"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
)

// QueryHistogramVec get the sql query time
var QueryHistogramVec *prometheus.HistogramVec

type mysqlExecutor struct {
	db *sql.DB
	*baseError
}

func newMysql(cfg *DBConfig, sqlMode *string) (Executor, error) {
	db, err := pkgsql.OpenDBWithSQLMode("mysql", cfg.Host, cfg.Port, cfg.User, cfg.Password, sqlMode)
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
