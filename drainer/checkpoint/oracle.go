// Copyright 2021 PingCAP, Inc.
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
	"context"
	dbsql "database/sql"
	"encoding/json"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	// mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	"github.com/pingcap/tidb-binlog/pkg/util"
)

// OracleCheckPoint is a local savepoint struct for Oracle
type OracleCheckPoint struct {
	sync.RWMutex
	closed          bool
	clusterID       uint64
	initialCommitTS int64

	db     *dbsql.DB
	schema string
	table  string

	ConsistentSaved bool             `toml:"consistent" json:"consistent"`
	CommitTS        int64            `toml:"commitTS" json:"commitTS"`
	TsMap           map[string]int64 `toml:"ts-map" json:"ts-map"`
	Version         int64            `toml:"schema-version" json:"schema-version"`
}

var _ CheckPoint = &OracleCheckPoint{}

var sqlOpenOracleDB = loader.CreateOracleDB

func newOracle(cfg *Config) (CheckPoint, error) {
	setDefaultConfig(cfg)

	if cfg.Db.TLS != nil {
		log.Info("enable TLS for saving checkpoint")
	}

	db, err := sqlOpenOracleDB(cfg.Db.User, cfg.Db.Password, cfg.Db.Host, cfg.Db.Port, cfg.Db.OracleServiceName, cfg.Db.OracleConnectString)
	if err != nil {
		return nil, errors.Annotate(err, "open db failed")
	}

	sp := &OracleCheckPoint{
		db:              db,
		clusterID:       cfg.ClusterID,
		initialCommitTS: cfg.InitialCommitTS,
		schema:          cfg.Schema,
		table:           cfg.Table,
		TsMap:           make(map[string]int64),
	}

	sql := genCheckTableIsExist2o(sp)

	var tableName string
	err = db.QueryRow(sql).Scan(&tableName)
	switch {
	case err == dbsql.ErrNoRows:
		sql = genCreateTable2o(sp)
		if _, err = db.Exec(sql); err != nil {
			return nil, errors.Annotatef(err, "create checkpoint table failed, sql: %s", sql)
		}
	case err != nil:
		return nil, errors.Annotatef(err, "exec failed, sql: %s", sql)
	}

	if sp.clusterID == 0 {
		id, err := getClusterID(db, sp.schema, sp.table)
		if err != nil {
			return nil, errors.Trace(err)
		}

		log.Info("set cluster id", zap.Uint64("id", id))
		sp.clusterID = id
	}

	err = sp.Load()
	return sp, errors.Trace(err)
}

// Load implements CheckPoint.Load interface
func (sp *OracleCheckPoint) Load() error {
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
	selectSQL := genSelectSQL2o(sp)
	err := sp.db.QueryRow(selectSQL).Scan(&str)
	switch {
	case err == dbsql.ErrNoRows:
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

// Save implements checkpoint.Save interface
func (sp *OracleCheckPoint) Save(ts, secondaryTS int64, consistent bool, version int64) error {
	sp.Lock()
	defer sp.Unlock()

	if sp.closed {
		return errors.Trace(ErrCheckPointClosed)
	}

	sp.CommitTS = ts
	sp.ConsistentSaved = consistent
	if version > sp.Version {
		sp.Version = version
	}

	if secondaryTS > 0 {
		sp.TsMap["primary-ts"] = ts
		sp.TsMap["secondary-ts"] = secondaryTS
	}

	b, err := json.Marshal(sp)
	if err != nil {
		return errors.Annotate(err, "json marshal failed")
	}

	sql := genReplaceSQL2o(sp, string(b))
	return util.RetryContext(context.TODO(), 5, time.Second, 1, func(context.Context) error {
		_, err = sp.db.Exec(sql)
		if err != nil {
			return errors.Annotatef(err, "query sql failed: %s", sql)
		}
		return nil
	})
}

// IsConsistent implements CheckPoint interface
func (sp *OracleCheckPoint) IsConsistent() bool {
	sp.RLock()
	defer sp.RUnlock()

	return sp.ConsistentSaved
}

// TS implements CheckPoint.TS interface
func (sp *OracleCheckPoint) TS() int64 {
	sp.RLock()
	defer sp.RUnlock()

	return sp.CommitTS
}

// SchemaVersion implements CheckPoint.SchemaVersion interface.
func (sp *OracleCheckPoint) SchemaVersion() int64 {
	sp.RLock()
	defer sp.RUnlock()

	return sp.Version
}

// Close implements CheckPoint.Close interface
func (sp *OracleCheckPoint) Close() error {
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
