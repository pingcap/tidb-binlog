package drainer

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	tddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	tmysql "github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

// ExecuteSQL wraps the GenInsertSQLs's ExecuteSQL method
func executeSQLs(db *sql.DB, sqls []string, args [][]interface{}, retry bool) error {
	if len(sqls) == 0 {
		return nil
	}

	var (
		err error
		txn *sql.Tx
	)

	retryCount := 1
	if retry {
		retryCount = maxRetryCount
	}

LOOP:
	for i := 0; i < retryCount; i++ {
		if i > 0 {
			log.Warnf("exec sql retry %d - %v - %v", i, sqls, args)
			time.Sleep(retryTimeout)
		}

		txn, err = db.Begin()
		if err != nil {
			log.Errorf("exec sqls[%v] begin failed %v", sqls, errors.ErrorStack(err))
			continue
		}

		for i := range sqls {
			log.Debugf("[exec][sql]%s[args]%v", sqls[i], args[i])

			_, err = txn.Exec(sqls[i], args[i]...)
			if err != nil {
				log.Warnf("[exec][sql]%s[args]%v[error]%v", sqls[i], args[i], err)
				rerr := txn.Rollback()
				if rerr != nil {
					log.Errorf("[exec][sql]%s[args]%v[error]%v", sqls[i], args[i], rerr)
				}
				continue LOOP
			}
		}

		err = txn.Commit()
		if err != nil {
			log.Errorf("exec sqls[%v] commit failed %v", sqls, errors.ErrorStack(err))
			continue
		}

		return nil
	}

	if err != nil {
		log.Errorf("exec sqls[%v] failed %v", sqls, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	return errors.Errorf("exec sqls[%v] failed", sqls)
}

func ignoreDDLError(err error) bool {
	mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError)
	if !ok {
		return false
	}

	errCode := terror.ErrCode(mysqlErr.Number)
	switch errCode {
	case infoschema.ErrDatabaseExists.Code(), infoschema.ErrDatabaseNotExists.Code(), infoschema.ErrDatabaseDropExists.Code(),
		infoschema.ErrTableExists.Code(), infoschema.ErrTableNotExists.Code(), infoschema.ErrTableDropExists.Code(),
		infoschema.ErrColumnExists.Code(), infoschema.ErrColumnNotExists.Code(),
		infoschema.ErrIndexExists.Code(), tddl.ErrCantDropFieldOrKey.Code():
		return true
	default:
		return false
	}
}

func openDB(username string, password string, host string, port int, proto string) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8&multiStatements=true", username, password, host, port)
	db, err := sql.Open(proto, dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, nil
}

func closeDB(db *sql.DB) error {
	return errors.Trace(db.Close())
}

func adjustColumn(table *model.TableInfo, job *model.Job) error {
	offsetChanged := make(map[int]int)
	offset := 0
	col := &model.ColumnInfo{}

	err := job.DecodeArgs(col, nil, &offset)
	if err != nil || col.Name.L != table.Columns[offset].Name.L {
		return errors.Trace(err)
	}

	for i := offset + 1; i < len(table.Columns); i++ {
		offsetChanged[table.Columns[i].Offset] = i
		table.Columns[i].Offset = i
	}
	table.Columns[offset].Offset = offset

	for _, idx := range table.Indices {
		for _, col := range idx.Columns {
			newOffset, ok := offsetChanged[col.Offset]
			if ok {
				col.Offset = newOffset
			}
		}
	}

	return nil
}

func adjustTableIndex(table *model.TableInfo, job *model.Job, isAdd bool) error {
	var indexName model.CIStr
	var indexInfo *model.IndexInfo
	var err error

	if isAdd {
		err = job.DecodeArgs(nil, &indexName, nil, nil)
	} else {
		err = job.DecodeArgs(&indexName)
	}

	if err != nil {
		return errors.Trace(err)
	}

	newIndices := make([]*model.IndexInfo, 0, len(table.Indices))
	for _, idx := range table.Indices {
		if idx.Name.L != indexName.L {
			newIndices = append(newIndices, idx)
		} else {
			indexInfo = idx
		}
	}

	if indexInfo == nil {
		return errors.NotFoundf("table %s(%d) 's index %s", table.Name, table.ID, indexName)
	}

	if isAdd {
		addIndexColumnFlag(table, indexInfo)
		return nil
	}

	table.Indices = newIndices

	dropIndexColumnFlag(table, indexInfo)
	return nil
}

func addIndexColumnFlag(table *model.TableInfo, indexInfo *model.IndexInfo) error {

	col := indexInfo.Columns[0]

	if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		table.Columns[col.Offset].Flag |= tmysql.UniqueKeyFlag
	} else {
		table.Columns[col.Offset].Flag |= tmysql.MultipleKeyFlag
	}

	return nil
}

func dropIndexColumnFlag(table *model.TableInfo, indexInfo *model.IndexInfo) {
	col := indexInfo.Columns[0]

	if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		table.Columns[col.Offset].Flag &= ^uint(tmysql.UniqueKeyFlag)
	} else {
		table.Columns[col.Offset].Flag &= ^uint(tmysql.MultipleKeyFlag)
	}

	for _, index := range table.Indices {
		if index.Name.L == indexInfo.Name.L {
			continue
		}

		if index.Columns[0].Name.L != col.Name.L {
			continue
		}

		addIndexColumnFlag(table, index)
	}
}
