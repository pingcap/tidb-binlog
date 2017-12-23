package executor

import (
	"database/sql"
	"fmt"
	"time"

	// mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

var (
	maxDMLRetryCount = 100
	maxDDLRetryCount = 5
	retryWaitTime    = 3 * time.Second
)

// DBConfig is the DB configuration.
type DBConfig struct {
	Host          string `toml:"host" json:"host"`
	User          string `toml:"user" json:"user"`
	Password      string `toml:"password" json:"password"`
	Port          int    `toml:"port" json:"port"`
	BinlogFileDir string `toml:"dir" json:"dir"`
}

func executeSQLs(db *sql.DB, sqls []string, args [][]interface{}, isDDL bool) error {
	start := time.Now()
	defer func() {
		cost := time.Now().Sub(start)
		log.Debugf("execute sqls count: %d, isDDL: %v, cost time: %v", len(sqls), isDDL, cost)
	}()
	if len(sqls) == 0 {
		return nil
	}

	retryCount := maxDMLRetryCount
	if isDDL {
		retryCount = maxDDLRetryCount
	}

	var err error
	for i := 0; i < retryCount; i++ {
		if i > 0 {
			log.Warnf("exec sql retry %d - %v", i, sqls)
			time.Sleep(retryWaitTime)
		}

		err = appleTxn(db, sqls, args)
		if err == nil {
			return nil
		}
	}

	return errors.Trace(err)
}

func appleTxn(db *sql.DB, sqls []string, args [][]interface{}) error {
	txn, err := db.Begin()
	if err != nil {
		log.Errorf("exec sqls[%v] begin failed %v", sqls, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	for i := range sqls {
		log.Debugf("[exec][sql]%s[args]%v", sqls[i], args[i])

		_, err = txn.Exec(sqls[i], args[i]...)
		if err != nil {
			log.Warnf("[exec][sql]%s[args]%v[error]%v", sqls[i], args[i], err)
			rerr := txn.Rollback()
			if rerr != nil {
				log.Errorf("[rollback][error]%v", rerr)
			}
			return errors.Trace(err)
		}
	}

	err = txn.Commit()
	if err != nil {
		log.Errorf("exec sqls[%v] commit failed %v", sqls, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	return nil
}

func openDB(proto string, host string, port int, username string, password string) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8&multiStatements=true", username, password, host, port)
	db, err := sql.Open(proto, dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, nil
}
