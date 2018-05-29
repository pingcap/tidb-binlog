package executor

import (
	"database/sql"
	"github.com/juju/errors"
	_ "github.com/kshvakov/clickhouse"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
)

type flashExecutor struct {
	dbs []*sql.DB
}

func newFlash(cfg *DBConfig) (Executor, error) {
	hostAndPorts, err := pkgsql.ParseCHHosts(cfg.Host)
	if err != nil {
		return nil, errors.Trace(err)
	}
	dbs := make([]*sql.DB, 0)
	for _, hostAndPort := range hostAndPorts {
		db, err := pkgsql.OpenCH("clickhouse", hostAndPort.Host, hostAndPort.Port, cfg.User, cfg.Password)
		if err != nil {
			return nil, errors.Trace(err)
		}
		dbs = append(dbs, db)
	}

	return &flashExecutor{
		dbs: dbs,
	}, nil
}

// partition must be a index of dbs
func (m *flashExecutor) partition(key int64) int {
	return int(key % int64(len(m.dbs)))
}

func (m *flashExecutor) Execute(sqls []string, args [][]interface{}, commitTSs []int64, isDDL bool) error {
	var err error

	// TODO: Need trx protection across multiple nodes
	// Actually here we need to store tables into tidb and read it back from spark
	// only by this way
	if isDDL {
		for _, db := range m.dbs {
			err = pkgsql.ExecuteSQLs(db, sqls, args, isDDL)
			if err != nil {
				return errors.Trace(err)
			}
		}
	} else {
		rowsMap := make(map[int][][]interface{})
		sqlsMap := make(map[int][]string)
		for i, row := range args {
			hashKey := m.partition(row[0].(int64))
			rowsMap[hashKey] = append(rowsMap[hashKey], row[1:len(row)])
			sqlsMap[hashKey] = append(sqlsMap[hashKey], sqls[i])
		}
		for i, db := range m.dbs {
			err = pkgsql.ExecuteSQLs(db, sqlsMap[i], rowsMap[i], isDDL)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	return errors.Trace(err)
}

func (m *flashExecutor) Close() error {
	for _, db := range m.dbs {
		err := db.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
