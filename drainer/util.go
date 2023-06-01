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
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"path"
	"sort"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	baf "github.com/pingcap/tidb/util/filter"
	router "github.com/pingcap/tidb/util/table-router"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
)

const (
	maxKafkaMsgSize = 1 << 30
	// max grpc message size, leave 4MB as buffer. Because when grpc decompresses messages, it will leave a few buffer
	// for this, which overflows the int64: https://github.com/grpc/grpc-go/blob/v1.44.0/rpc_util.go#L742
	maxGrpcMsgSize = int(^uint(0)>>1) - 4*1024*1024
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
		buildCheckPointCfg(checkpointCfg, toCheckpoint.Type, toCheckpoint.Host, toCheckpoint.User, toCheckpoint.Password, toCheckpoint.Port, toCheckpoint.TLS, toCheckpoint.Table)
	case "oracle":
		buildOracleCheckpointCfg(checkpointCfg, toCheckpoint.Type, toCheckpoint.Host, toCheckpoint.User, toCheckpoint.Password, toCheckpoint.Port, toCheckpoint.TLS,
			toCheckpoint.OracleServiceName, toCheckpoint.OracleConnectString, toCheckpoint.Table)
		if len(checkpointCfg.Schema) != 0 && checkpointCfg.Db.User != checkpointCfg.Schema {
			return nil, errors.New("in oracle, user is like as a schema.so you must keep it same as schema.you can not set schema item in checkpoint configuration")
		}
	case "":
		switch cfg.SyncerCfg.DestDBType {
		case "mysql", "tidb":
			buildCheckPointCfg(checkpointCfg, cfg.SyncerCfg.DestDBType, cfg.SyncerCfg.To.Host, cfg.SyncerCfg.To.User, cfg.SyncerCfg.To.Password, cfg.SyncerCfg.To.Port, cfg.SyncerCfg.To.TLS, "")
		case "oracle":
			buildOracleCheckpointCfg(checkpointCfg, cfg.SyncerCfg.DestDBType, cfg.SyncerCfg.To.Host, cfg.SyncerCfg.To.User,
				cfg.SyncerCfg.To.Password, cfg.SyncerCfg.To.Port, cfg.SyncerCfg.To.TLS, cfg.SyncerCfg.To.OracleServiceName, cfg.SyncerCfg.To.OracleConnectString, "")
			if len(checkpointCfg.Schema) != 0 && checkpointCfg.Db.User != checkpointCfg.Schema {
				return nil, errors.New("in oracle, user is like as a schema.so you must keep it same as schema.you can not set schema item in checkpoint configuration")
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

func buildCheckPointCfg(checkpointCfg *checkpoint.Config, cfgType, host, user, password string, port int, tls *tls.Config, table string) {
	checkpointCfg.CheckpointType = cfgType
	checkpointCfg.Table = table
	checkpointCfg.Db = &checkpoint.DBConfig{
		Host:     host,
		User:     user,
		Password: password,
		Port:     port,
		TLS:      tls,
	}
}

func buildOracleCheckpointCfg(checkpointCfg *checkpoint.Config, cfgType, host, user, password string, port int, tls *tls.Config, serviceName, connectString, table string) {
	buildCheckPointCfg(checkpointCfg, cfgType, host, user, password, port, tls, table)
	checkpointCfg.Db.OracleServiceName = serviceName
	checkpointCfg.Db.OracleConnectString = connectString
}

func initializeSaramaGlobalConfig(kafkaMsgSize int32) {
	sarama.MaxResponseSize = kafkaMsgSize
	// add 1 to avoid confused log: Producer.MaxMessageBytes must be smaller than MaxRequestSize; it will be ignored
	if kafkaMsgSize < math.MaxInt32 {
		sarama.MaxRequestSize = kafkaMsgSize + 1
	} else {
		sarama.MaxRequestSize = kafkaMsgSize
	}
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
	jobs, err := ddl.GetAllHistoryDDLJobs(snapMeta)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// jobs from GetAllHistoryDDLJobs are sorted by job id, need sorted by schema version
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].BinlogInfo.SchemaVersion < jobs[j].BinlogInfo.SchemaVersion
	})

	return jobs, nil
}

// loadTableInfos loads all table infos after startTs
func loadTableInfos(tiStore kv.Storage, startTs int64) ([]*model.Job, error) {
	meta := getSnapshotMetaFromTs(tiStore, startTs)
	dbinfos, err := meta.ListDatabases()
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := make([]*model.Job, 0, len(dbinfos))
	version := int64(1)
	for _, dbinfo := range dbinfos {
		log.L().Info("load db info", zap.Stringer("db", dbinfo.Name), zap.Int64("version", version))
		jobs = append(jobs, mockCreateSchemaJob(dbinfo, version))
		version++
	}
	for _, dbinfo := range dbinfos {
		tableInfos, err := meta.ListTables(dbinfo.ID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(tableInfos) == 0 {
			continue
		}
		for _, tableInfo := range tableInfos {
			log.L().Debug("load table info", zap.Stringer("db", dbinfo.Name), zap.Stringer("table", tableInfo.Name), zap.Int64("version", version))
		}
		jobs = append(jobs, &model.Job{
			Type:     model.ActionCreateTables,
			State:    model.JobStateDone,
			SchemaID: dbinfo.ID,
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion:      version,
				MultipleTableInfos: tableInfos,
			},
		})
		version++
	}
	return jobs, nil
}

func getSnapshotMeta(tiStore kv.Storage) (*meta.Meta, error) {
	version, err := tiStore.CurrentVersion(oracle.GlobalTxnScope)
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapshot := tiStore.GetSnapshot(version)
	return meta.NewSnapshotMeta(snapshot), nil
}

func getSnapshotMetaFromTs(tiStore kv.Storage, ts int64) *meta.Meta {
	snapshot := tiStore.GetSnapshot(kv.NewVersion(uint64(ts)))
	return meta.NewSnapshotMeta(snapshot)
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

func getParser(sqlMode mysql.SQLMode) (p *parser.Parser) {
	p = parser.New()
	p.SetSQLMode(sqlMode)

	return
}

func genRouterAndBinlogEvent(cfg *SyncerConfig) (*router.Table, *bf.BinlogEvent, error) {
	var (
		routeRules  []*router.TableRule
		filterRules []*bf.BinlogEventRule
	)
	// filter rule name -> filter rule template
	eventFilterTemplateMap := make(map[string]bf.BinlogEventRule)
	if cfg.BinlogFilterRule != nil {
		for ruleName, rule := range cfg.BinlogFilterRule {
			ruleT := bf.BinlogEventRule{Action: bf.Ignore}
			if rule.IgnoreEvent != nil {
				events := make([]bf.EventType, len(*rule.IgnoreEvent))
				for i, eventStr := range *rule.IgnoreEvent {
					events[i] = bf.EventType(eventStr)
				}
				ruleT.Events = events
			}
			if rule.IgnoreSQL != nil {
				ruleT.SQLPattern = *rule.IgnoreSQL
			}
			eventFilterTemplateMap[ruleName] = ruleT
		}
	}

	// set route,blockAllowList,filter config
	doCnt := len(cfg.TableMigrateRule)
	doDBs := make([]string, doCnt)
	doTables := make([]*baf.Table, doCnt)
	for j, rule := range cfg.TableMigrateRule {
		// route
		if rule.Target != nil {
			routeRules = append(routeRules, &router.TableRule{
				SchemaPattern: rule.Source.Schema, TablePattern: rule.Source.Table,
				TargetSchema: rule.Target.Schema, TargetTable: rule.Target.Table,
			})
		}
		// filter
		if rule.BinlogFilterRule != nil {
			for _, name := range *rule.BinlogFilterRule {
				filterRule, ok := eventFilterTemplateMap[name] // NOTE: this return a copied value
				if !ok {
					return nil, nil, errors.Errorf("event filter rule name %s not found", name)
				}
				filterRule.SchemaPattern = rule.Source.Schema
				filterRule.TablePattern = rule.Source.Table
				filterRules = append(filterRules, &filterRule)
			}
		}
		// BlockAllowList
		doDBs[j] = rule.Source.Schema
		doTables[j] = &baf.Table{Schema: rule.Source.Schema, Name: rule.Source.Table}
	}
	filterRules = combineFilterRules(filterRules)
	var (
		tableRouter  *router.Table
		binlogFilter *bf.BinlogEvent
		err          error
	)
	if cfg.DestDBType == "oracle" {
		tableRouter, err = router.NewTableRouter(cfg.CaseSensitive, routeRules)
		if err != nil {
			return nil, nil, errors.Annotate(err, "generate table router error")
		}
	}
	binlogFilter, err = bf.NewBinlogEvent(cfg.CaseSensitive, filterRules)
	if err != nil {
		return nil, nil, errors.Annotate(err, "generate binlog event filter error")
	}
	return tableRouter, binlogFilter, nil
}

func combineFilterRules(filterRules []*bf.BinlogEventRule) []*bf.BinlogEventRule {
	rules := make([]*bf.BinlogEventRule, 0, len(filterRules)/2)
	rulesMap := make(map[string]map[string]*bf.BinlogEventRule)
	for _, rule := range filterRules {
		schema, table := rule.SchemaPattern, rule.TablePattern
		var (
			tableMap map[string]*bf.BinlogEventRule
			ok       bool
			ruleE    *bf.BinlogEventRule
		)
		if tableMap, ok = rulesMap[schema]; !ok {
			tableMap = make(map[string]*bf.BinlogEventRule)
			rulesMap[schema] = tableMap
		}
		if ruleE, ok = tableMap[table]; !ok {
			tableMap[table] = &bf.BinlogEventRule{
				Action:        bf.Ignore,
				SchemaPattern: schema,
				TablePattern:  table,
				Events:        append([]bf.EventType{}, rule.Events...),
				SQLPattern:    append([]string{}, rule.SQLPattern...),
			}
		} else {
			ruleE.Events = append(ruleE.Events, rule.Events...)
			ruleE.SQLPattern = append(ruleE.SQLPattern, rule.SQLPattern...)
		}
	}
	for _, tableMap := range rulesMap {
		for _, rule := range tableMap {
			rules = append(rules, rule)
		}
	}
	return rules
}

func mockCreateSchemaJob(dbInfo *model.DBInfo, schemaVersion int64) *model.Job {
	return &model.Job{
		Type:  model.ActionCreateSchema,
		State: model.JobStateDone,
		BinlogInfo: &model.HistoryInfo{
			SchemaVersion: schemaVersion,
			DBInfo:        dbInfo,
		},
	}
}
