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

package drainer

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"go.uber.org/zap"
)

const (
	maxMsgSize = 1024 * 1024 * 1024
)

// taskGroup is a wrapper of `sync.WaitGroup`.
type taskGroup struct {
	wg sync.WaitGroup
}

func (g *taskGroup) Go(name string, f func()) {
	g.start(name, f, false)
}

func (g *taskGroup) GoNoPanic(name string, f func()) {
	g.start(name, f, true)
}

func (g *taskGroup) start(name string, f func(), noPanic bool) {
	fName := zap.String("name", name)
	g.wg.Add(1)
	go func() {
		defer func() {
			if noPanic {
				if err := recover(); err != nil {
					log.Error("Recovered from panic",
						zap.Reflect("err", err),
						zap.Stack("real stack"),
						fName,
					)
				}
			}
			log.Info("Exit", fName)
			g.wg.Done()
		}()
		f()
	}()
}

func (g *taskGroup) Wait() {
	g.wg.Wait()
}

// GenCheckPointCfg returns an CheckPoint config instance
func GenCheckPointCfg(cfg *Config, id uint64) (*checkpoint.Config, error) {

	checkpointCfg := &checkpoint.Config{
		ClusterID:       id,
		InitialCommitTS: cfg.InitialCommitTS,
		CheckPointFile:  path.Join(cfg.DataDir, "savepoint"),
	}

	toCheckpoint := cfg.SyncerCfg.To.Checkpoint

	if toCheckpoint.Schema != "" {
		checkpointCfg.Schema = toCheckpoint.Schema
	}

	switch toCheckpoint.Type {
	case "mysql", "tidb":
		checkpointCfg.CheckpointType = toCheckpoint.Type
		checkpointCfg.Db = &checkpoint.DBConfig{
			Host:     toCheckpoint.Host,
			User:     toCheckpoint.User,
			Password: toCheckpoint.Password,
			Port:     toCheckpoint.Port,
		}
	case "":
		switch cfg.SyncerCfg.DestDBType {
		case "mysql", "tidb":
			checkpointCfg.CheckpointType = cfg.SyncerCfg.DestDBType
			checkpointCfg.Db = &checkpoint.DBConfig{
				Host:     cfg.SyncerCfg.To.Host,
				User:     cfg.SyncerCfg.To.User,
				Password: cfg.SyncerCfg.To.Password,
				Port:     cfg.SyncerCfg.To.Port,
			}
		case "pb", "file":
			checkpointCfg.CheckpointType = "file"
		case "kafka":
			checkpointCfg.CheckpointType = "file"
		case "flash":
			return nil, errors.New("the flash DestDBType is no longer supported")
		default:
			return nil, errors.Errorf("unknown DestDBType: %s", cfg.SyncerCfg.DestDBType)
		}
	default:
		return nil, errors.Errorf("unknown checkpoint type: %s", toCheckpoint.Type)
	}

	return checkpointCfg, nil
}

func initializeSaramaGlobalConfig() {
	sarama.MaxResponseSize = int32(maxMsgSize)
	// add 1 to avoid confused log: Producer.MaxMessageBytes must be smaller than MaxRequestSize; it will be ignored
	sarama.MaxRequestSize = int32(maxMsgSize) + 1
}

func getDDLJob(tiStore kv.Storage, id int64) (*model.Job, error) {
	snapMeta, err := getSnapshotMeta(tiStore)
	if err != nil {
		return nil, errors.Trace(err)
	}
	job, err := snapMeta.GetHistoryDDLJob(id)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return job, nil
}

// loadHistoryDDLJobs loads all history DDL jobs from TiDB
func loadHistoryDDLJobs(tiStore kv.Storage) ([]*model.Job, error) {
	snapMeta, err := getSnapshotMeta(tiStore)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs, err := snapMeta.GetAllHistoryDDLJobs()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// jobs from GetAllHistoryDDLJobs are sorted by job id, need sorted by schema version
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].BinlogInfo.SchemaVersion < jobs[j].BinlogInfo.SchemaVersion
	})

	return jobs, nil
}

func getSnapshotMeta(tiStore kv.Storage) (*meta.Meta, error) {
	version, err := tiStore.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapshot, err := tiStore.GetSnapshot(version)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return meta.NewSnapshotMeta(snapshot), nil
}

func genDrainerID(listenAddr string) (string, error) {
	urllis, err := url.Parse(listenAddr)
	if err != nil {
		return "", errors.Trace(err)
	}

	_, port, err := net.SplitHostPort(urllis.Host)
	if err != nil {
		return "", errors.Trace(err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "", errors.Trace(err)
	}

	return fmt.Sprintf("%s:%s", hostname, port), nil
}

// getParserFromSQLModeStr gets a parser and applies given sqlMode.
func getParserFromSQLModeStr(sqlMode string) (*parser.Parser, error) {
	mode, err := tmysql.GetSQLMode(sqlMode)
	if err != nil {
		return nil, err
	}

	parser2 := parser.New()
	parser2.SetSQLMode(mode)
	return parser2, nil
}

// collectDirFiles gets files in path.
func collectDirFiles(path string) (map[string]struct{}, error) {
	files := make(map[string]struct{})
	err := filepath.Walk(path, func(_ string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if f == nil {
			return nil
		}

		if f.IsDir() {
			return nil
		}

		name := strings.TrimSpace(f.Name())
		files[name] = struct{}{}
		return nil
	})

	return files, err
}

// getDBFromDumpFilename extracts db name from dump filename.
func getDBFromDumpFilename(filename string) (db string, ok bool) {
	if !strings.HasSuffix(filename, "-schema-create.sql") {
		return "", false
	}

	idx := strings.LastIndex(filename, "-schema-create.sql")
	return filename[:idx], true
}

// getTableFromDumpFilename extracts db and table name from dump filename.
func getTableFromDumpFilename(filename string) (db, table string, ok bool) {
	if !strings.HasSuffix(filename, "-schema.sql") {
		return "", "", false
	}

	idx := strings.LastIndex(filename, "-schema.sql")
	name := filename[:idx]
	fields := strings.Split(name, ".")
	if len(fields) != 2 {
		return "", "", false
	}
	return fields[0], fields[1], true
}

type schemaKey struct {
	schemaName string
	tableName  string
}

type schemaInfo struct {
	stmt string
	id   int64
}

func getStmtFromFile(file string) (string, error) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return "", err
	}
	stmts := bytes.Split(content, []byte(";"))
	for _, stmt := range stmts {
		stmt = bytes.TrimSpace(stmt)
		if len(stmt) == 0 || bytes.HasPrefix(stmt, []byte("/*")) {
			continue
		}
		return string(stmt), nil
	}
	return "", errors.New("no stmt found")
}

func getSchemaIDByName(schemaName string, dbIDMaps map[string]int64) (int64, bool) {
	id, ok := dbIDMaps[schemaName]
	return id, ok
}

func getTableIDByName(schemaName, tableName string, tblIDMap map[string]map[string]int64) (int64, bool) {

	id, ok := dbIDMaps[schemaName]
	return id, ok
}

func loadSchemaIDsFromDump(dir string) (map[string]int64, error) {
	schemaIDs := map[string]int64{}
	file := path.Join(dir, "schema")
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return schemaIDs, err
	}
	var dbs []*model.DBInfo
	err = json.Unmarshal(content, &dbs)
	if err != nil {
		return schemaIDs, err
	}
	for _, dbInfo := range dbs {
		schemaIDs[dbInfo.Name.O] = dbInfo.ID
	}
	return schemaIDs, nil
}

func loadTableIDsFromDump(dir string) (map[string]map[string]int64, error) {
	tableIDs := make(map[string]map[string]int64)
	fileName := path.Join(dir, "result.000000000.csv")
	file, err := os.Open(fileName)
	if err != nil {
		return tableIDs, err
	}
	reader := csv.NewReader(file)
	for {
		records, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Trace(err)
		}
		if len(records) != 3 {
			return nil, errors.Errorf("invalid csv record [%s]", strings.Join(records, ","))
		}
		dbName, tbName := strings.Trim(records[0], "\""), strings.Trim(records[1], "\"")
		tableID, err := strconv.ParseInt(records[2], 10, 64)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if _, ok := tableIDs[dbName]; !ok {
			tableIDs[dbName] = make(map[string]int64)
		}
		tableIDs[dbName][tbName] = tableID
	}
	return tableIDs, nil
}

func loadInfosFromDump(dir string, dbIDMaps map[string]int64, tblIDMaps map[string]map[string]int64) (map[schemaKey]schemaInfo, map[schemaKey]schemaInfo, error) {
	var (
		dbInfos = make(map[schemaKey]schemaInfo)
		tbInfos = make(map[schemaKey]schemaInfo)
	)
	files, err := collectDirFiles(dir)
	if err != nil {
		log.Error("fail to get dump files", zap.Error(err))
		return nil, nil, err
	}
	for f := range files {
		stmt, err := getStmtFromFile(filepath.Join(dir, f))
		if err != nil {
			log.L().Error("failed to get stmt from file", zap.String("dir", dir), zap.String("file", f), zap.Error(err))
			return nil, nil, err
		}
		if db, ok := getDBFromDumpFilename(f); ok {
			id, ok := dbIDMaps[db]
			if !ok {
				log.L().Error("database ID not found", zap.String("database", db))
			}
			dbInfos[schemaKey{schemaName: db}] = schemaInfo{stmt: stmt, id: id}
		} else if db, tb, ok := getTableFromDumpFilename(f); ok {
			id, ok := getTableIDByName(db, tb, tblIDMaps)
			if !ok {
				log.L().Error("table ID not found", zap.String("database", db), zap.String("table", tb))
			}
			tbInfos[schemaKey{schemaName: db, tableName: tb}] = schemaInfo{stmt: stmt, id: id}
		}
		// do we need handle view here?
	}
	return dbInfos, tbInfos, nil
}
