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
	"crypto/tls"
	"database/sql"
	stderrors "errors"
	"fmt"
	"strings"

	// mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
)

// ErrNoCheckpointItem represents there's no any checkpoint item and the cluster id must be specified
// for the mysql checkpoint type.
var ErrNoCheckpointItem = stderrors.New("no any checkpoint item")

// DBConfig is the DB configuration.
type DBConfig struct {
	Host     string      `toml:"host" json:"host"`
	User     string      `toml:"user" json:"user"`
	Password string      `toml:"password" json:"password"`
	Port     int         `toml:"port" json:"port"`
	TLS      *tls.Config `toml:"-" json:"-"`
	//for oracle database
	OracleServiceName   string `toml:"oracle-service-name" json:"oracle-service-name"`
	OracleConnectString string `toml:"oracle-connect-string" json:"oracle-connect-string"`
}

// Config is the savepoint configuration
type Config struct {
	CheckpointType string

	Db     *DBConfig
	Schema string
	Table  string

	ClusterID       uint64
	InitialCommitTS int64
	CheckPointFile  string `toml:"dir" json:"dir"`
}

func setDefaultConfig(cfg *Config) {
	if cfg.Db == nil {
		cfg.Db = new(DBConfig)
	}
	if cfg.Db.Host == "" {
		cfg.Db.Host = "127.0.0.1"
	}
	if cfg.Db.Port == 0 {
		cfg.Db.Port = 3306
	}
	if cfg.Db.User == "" {
		cfg.Db.User = "root"
	}
	if cfg.Schema == "" {
		if cfg.CheckpointType == "oracle" {
			cfg.Schema = cfg.Db.User
		} else {
			cfg.Schema = "tidb_binlog"
		}
	}
	if cfg.Table == "" {
		if cfg.CheckpointType == "oracle" {
			cfg.Table = "tidb_binlog_checkpoint"
		} else {
			cfg.Table = "checkpoint"
		}
	}
}

func genCreateSchema(sp *MysqlCheckPoint) string {
	return fmt.Sprintf("create schema if not exists %s", sp.schema)
}

func genCreateTable(sp *MysqlCheckPoint) string {
	return fmt.Sprintf("create table if not exists %s.%s(clusterID bigint unsigned primary key, checkPoint MEDIUMTEXT)", sp.schema, sp.table)
}

func genReplaceSQL(sp *MysqlCheckPoint, str string) string {
	return fmt.Sprintf("replace into %s.%s values(%d, '%s')", sp.schema, sp.table, sp.clusterID, str)
}

func genCheckTableIsExist2o(sp *OracleCheckPoint) string {
	tableName := strings.ToUpper(sp.table)
	return fmt.Sprintf("select table_name from user_tables where table_name='%s'", tableName)
}

func genCreateTable2o(sp *OracleCheckPoint) string {
	return fmt.Sprintf("create table %s.%s (clusterID number primary key, checkPoint varchar2(4000))", sp.schema, sp.table)
}

func genReplaceSQL2o(sp *OracleCheckPoint, str string) string {
	return fmt.Sprintf("merge into %s.%s t using (select %d clusterID, '%s' checkPoint from dual) temp on(t.clusterID=temp.clusterID) "+
		"when matched then "+
		"update set t.checkPoint=temp.checkPoint "+
		"when not matched then "+
		"insert values(temp.clusterID,temp.checkPoint)", sp.schema, sp.table, sp.clusterID, str)
}

// getClusterID return the cluster id iff the checkpoint table exist only one row.
func getClusterID(db *sql.DB, schema string, table string) (id uint64, err error) {
	sqlQuery := fmt.Sprintf("select clusterID from %s.%s", schema, table)
	rows, err := db.Query(sqlQuery)
	if err != nil {
		return 0, errors.Trace(err)
	}

	for rows.Next() {
		if id > 0 {
			return 0, errors.New("there are multi row in checkpoint table")
		}

		err = rows.Scan(&id)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	if rows.Err() != nil {
		return 0, errors.Trace(rows.Err())
	}

	if id == 0 {
		return 0, ErrNoCheckpointItem
	}

	return
}

func genSelectSQL(sp *MysqlCheckPoint) string {
	return fmt.Sprintf("select checkPoint from %s.%s where clusterID = %d", sp.schema, sp.table, sp.clusterID)
}

func genSelectSQL2o(sp *OracleCheckPoint) string {
	return fmt.Sprintf("select checkPoint from %s.%s where clusterID = %d", sp.schema, sp.table, sp.clusterID)
}
