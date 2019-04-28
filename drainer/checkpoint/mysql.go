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

package checkpoint

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	// mysql driver
	_ "github.com/go-sql-driver/mysql"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	tmysql "github.com/pingcap/tidb/mysql"
)

// MysqlCheckPoint is a local savepoint struct for mysql
type MysqlCheckPoint struct {
	sync.RWMutex
	closed          bool
	clusterID       uint64
	initialCommitTS int64

	db       *sql.DB
	schema   string
	table    string
	saveTime time.Time
	// type, tidb or mysql
	tp string

	CommitTS int64            `toml:"commitTS" json:"commitTS"`
	TsMap    map[string]int64 `toml:"ts-map" json:"ts-map"`

	snapshot time.Time
}

var sqlOpenDB = pkgsql.OpenDB

func newMysql(tp string, cfg *Config) (CheckPoint, error) {
	if err := checkConfig(cfg); err != nil {
		return nil, errors.Annotate(err, "check config failed")
	}

	db, err := sqlOpenDB("mysql", cfg.Db.Host, cfg.Db.Port, cfg.Db.User, cfg.Db.Password)
	if err != nil {
		return nil, errors.Annotate(err, "open db failed")
	}

	sp := &MysqlCheckPoint{
		db:              db,
		clusterID:       cfg.ClusterID,
		initialCommitTS: cfg.InitialCommitTS,
		schema:          cfg.Schema,
		table:           cfg.Table,
		tp:              tp,
		TsMap:           make(map[string]int64),
		saveTime:        time.Now(),
	}

	sql := genCreateSchema(sp)
	if _, err = db.Exec(sql); err != nil {
		return nil, errors.Annotatef(err, "exec failed, sql: %s", sql)
	}

	sql = genCreateTable(sp)
	if _, err = db.Exec(sql); err != nil {
		return nil, errors.Annotatef(err, "exec failed, sql: %s", sql)
	}

	err = sp.Load()
	return sp, errors.Trace(err)
}

// Load implements CheckPoint.Load interface
func (sp *MysqlCheckPoint) Load() error {
	sp.Lock()
	defer sp.Unlock()

	if sp.closed {
		return errors.Trace(ErrCheckPointClosed)
	}

	defer func() {
		if sp.CommitTS == 0 {
			sp.CommitTS = sp.initialCommitTS
		}
	}()

	var str string
	selectSQL := genSelectSQL(sp)
	err := sp.db.QueryRow(selectSQL).Scan(&str)
	switch {
	case err == sql.ErrNoRows:
		sp.CommitTS = sp.initialCommitTS
		return nil
	case err != nil:
		return errors.Annotatef(err, "QueryRow failed, sql: %s", selectSQL)
	}

	if err := json.Unmarshal([]byte(str), sp); err != nil {
		return errors.Trace(err)
	}

	return nil
}

var getTidbPos = pkgsql.GetTidbPosition

// Save implements checkpoint.Save interface
func (sp *MysqlCheckPoint) Save(ts int64) error {
	sp.Lock()
	defer sp.Unlock()

	if sp.closed {
		return errors.Trace(ErrCheckPointClosed)
	}

	sp.CommitTS = ts
	sp.saveTime = time.Now()

	// we don't need update tsMap every time.
	if sp.tp == "tidb" && time.Since(sp.snapshot) > time.Minute {
		sp.snapshot = time.Now()
		slaveTS, err := getTidbPos(sp.db)
		if err != nil {
			// if tidb dont't support `show master status`, will return 1105 ErrUnknown error
			errCode, ok := pkgsql.GetSQLErrCode(err)
			if !ok || int(errCode) != tmysql.ErrUnknown {
				log.Warn("get ts from slave cluster failed", zap.Error(err))
			}
		} else {
			sp.TsMap["master-ts"] = ts
			sp.TsMap["slave-ts"] = slaveTS
		}
	}

	b, err := json.Marshal(sp)
	if err != nil {
		return errors.Annotate(err, "json marshal failed")
	}

	sql := genReplaceSQL(sp, string(b))
	_, err = sp.db.Exec(sql)
	if err != nil {
		return errors.Annotatef(err, "query sql failed: %s", sql)
	}

	return nil
}

// Check implements CheckPoint.Check interface
func (sp *MysqlCheckPoint) Check(int64) bool {
	sp.RLock()
	defer sp.RUnlock()

	if sp.closed {
		return false
	}

	return time.Since(sp.saveTime) >= maxSaveTime
}

// TS implements CheckPoint.TS interface
func (sp *MysqlCheckPoint) TS() int64 {
	sp.RLock()
	defer sp.RUnlock()

	return sp.CommitTS
}

// Close implements CheckPoint.Close interface
func (sp *MysqlCheckPoint) Close() error {
	sp.Lock()
	defer sp.Unlock()

	if sp.closed {
		return errors.Trace(ErrCheckPointClosed)
	}

	err := sp.db.Close()
	if err == nil {
		sp.closed = true
	}
	return errors.Trace(err)
}

// String implements CheckPoint.String interface
func (sp *MysqlCheckPoint) String() string {
	ts := sp.TS()
	return fmt.Sprintf("binlog commitTS = %d", ts)
}
