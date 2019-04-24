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

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/flash"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
)

// FlashCheckPoint is a local savepoint struct for flash
type FlashCheckPoint struct {
	sync.RWMutex
	closed          bool
	clusterID       uint64
	initialCommitTS int64

	db       *sql.DB
	schema   string
	table    string
	metaCP   *flash.MetaCheckpoint
	saveTime time.Time

	CommitTS int64 `toml:"commitTS" json:"commitTS"`
}

func checkFlashConfig(cfg *Config) {
	if cfg == nil {
		return
	}
	if cfg.Db == nil {
		cfg.Db = new(DBConfig)
	}
	if cfg.Db.Host == "" {
		cfg.Db.Host = "127.0.0.1"
	}
	if cfg.Db.Port == 0 {
		cfg.Db.Port = 9000
	}
	if cfg.Schema == "" {
		cfg.Schema = "tidb_binlog"
	}
	if cfg.Table == "" {
		cfg.Table = "checkpoint"
	}
}

var openCH = pkgsql.OpenCH

func newFlash(cfg *Config) (CheckPoint, error) {
	checkFlashConfig(cfg)

	hostAndPorts, err := pkgsql.ParseCHAddr(cfg.Db.Host)
	if err != nil {
		return nil, errors.Trace(err)
	}

	db, err := openCH(hostAndPorts[0].Host, hostAndPorts[0].Port, cfg.Db.User, cfg.Db.Password, "", 0)
	if err != nil {
		log.Errorf("open database error %v", err)
		return nil, errors.Trace(err)
	}

	sp := &FlashCheckPoint{
		db:              db,
		clusterID:       cfg.ClusterID,
		initialCommitTS: cfg.InitialCommitTS,
		schema:          cfg.Schema,
		table:           cfg.Table,
		metaCP:          flash.GetInstance(),
		saveTime:        time.Now(),
	}

	sql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", sp.schema)
	if _, err = execSQL(db, sql); err != nil {
		log.Errorf("Create database error %v", err)
		return sp, errors.Trace(err)
	}

	sql = fmt.Sprintf("ATTACH TABLE IF NOT EXISTS `%s`.`%s`(`clusterid` UInt64, `checkpoint` String) ENGINE MutableMergeTree((`clusterid`), 8192)", sp.schema, sp.table)
	if _, err = execSQL(db, sql); err != nil {
		log.Errorf("Create table error %v", err)
		return nil, errors.Trace(err)
	}

	err = sp.Load()
	return sp, errors.Trace(err)
}

// Load implements CheckPoint.Load interface
func (sp *FlashCheckPoint) Load() error {
	sp.Lock()
	defer sp.Unlock()

	if sp.closed {
		return errors.Trace(ErrCheckPointClosed)
	}

	sql := fmt.Sprintf("SELECT `checkpoint` from `%s`.`%s` WHERE `clusterid` = %d", sp.schema, sp.table, sp.clusterID)
	rows, err := querySQL(sp.db, sql)
	if err != nil {
		log.Errorf("select checkPoint error %v", err)
		return errors.Trace(err)
	}

	var str string
	for rows.Next() {
		err = rows.Scan(&str)
		if err != nil {
			log.Errorf("rows Scan error %v", err)
			return errors.Trace(err)
		}
	}

	if len(str) == 0 {
		sp.CommitTS = sp.initialCommitTS
		return nil
	}

	err = json.Unmarshal([]byte(str), sp)
	if err != nil {
		return errors.Trace(err)
	}

	if sp.CommitTS == 0 {
		sp.CommitTS = sp.initialCommitTS
	}
	return nil
}

// Save implements checkpoint.Save interface
func (sp *FlashCheckPoint) Save(ts int64) error {
	sp.Lock()
	defer sp.Unlock()

	if sp.closed {
		return errors.Trace(ErrCheckPointClosed)
	}

	sp.saveTime = time.Now()

	// Init CP using metaCP's safe CP.
	forceSave, ok, safeTS := sp.metaCP.PopSafeCP()
	if forceSave {
		// If force save, use the CP passed in.
		safeTS = ts
	} else if !ok {
		return nil
	}

	sp.CommitTS = safeTS

	b, err := json.Marshal(sp)
	if err != nil {
		log.Errorf("Json Marshal error %v", err)
		return errors.Trace(err)
	}

	sql := fmt.Sprintf("IMPORT INTO `%s`.`%s` (`clusterid`, `checkpoint`) VALUES(?, ?)", sp.schema, sp.table)
	sqls := []string{sql}
	args := [][]interface{}{{sp.clusterID, b}}
	err = pkgsql.ExecuteSQLs(sp.db, sqls, args, false)

	return errors.Trace(err)
}

// Check implements CheckPoint.Check interface
func (sp *FlashCheckPoint) Check(ts int64) bool {
	sp.RLock()
	defer sp.RUnlock()

	if sp.closed {
		return false
	}

	sp.metaCP.PushPendingCP(ts)

	return time.Since(sp.saveTime) >= maxSaveTime
}

// TS implements CheckPoint.TS interface
func (sp *FlashCheckPoint) TS() int64 {
	sp.RLock()
	defer sp.RUnlock()

	return sp.CommitTS
}

// Close implements CheckPoint.Close interface.
func (sp *FlashCheckPoint) Close() error {
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
func (sp *FlashCheckPoint) String() string {
	ts := sp.TS()
	return fmt.Sprintf("binlog commitTS = %d", ts)
}
