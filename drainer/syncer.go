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
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/parser/ast"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	baf "github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tipb/go-binlog"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	dsync "github.com/pingcap/tidb-binlog/drainer/sync"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/filter"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	"github.com/pingcap/tidb-binlog/pkg/util"
)

// runWaitThreshold is the expected time for `Syncer.run` to quit
// normally, we take record if it takes longer than this value.
var runWaitThreshold = 10 * time.Second

// Syncer converts tidb binlog to the specified DB sqls, and sync it to target DB
type Syncer struct {
	schema *Schema
	cp     checkpoint.CheckPoint

	cfg *SyncerConfig

	input chan *binlogItem

	filter *filter.Filter

	loopbackSync *loopbacksync.LoopBackSync

	// last time we successfully sync binlog item to downstream
	lastSyncTime time.Time

	dsyncer dsync.Syncer

	shutdown chan struct{}
	closed   chan struct{}

	tableRouter  *router.Table
	binlogFilter *bf.BinlogEvent
}

// NewSyncer returns a Drainer instance
func NewSyncer(cp checkpoint.CheckPoint, cfg *SyncerConfig, jobs []*model.Job) (*Syncer, error) {
	syncer := new(Syncer)
	syncer.cfg = cfg
	syncer.cp = cp
	syncer.input = make(chan *binlogItem, maxBinlogItemCount)
	syncer.lastSyncTime = time.Now()
	syncer.shutdown = make(chan struct{})
	syncer.closed = make(chan struct{})

	var ignoreDBs []string
	if len(cfg.IgnoreSchemas) > 0 {
		ignoreDBs = strings.Split(cfg.IgnoreSchemas, ",")
	}
	syncer.filter = filter.NewFilter(ignoreDBs, cfg.IgnoreTables, cfg.DoDBs, cfg.DoTables)
	syncer.loopbackSync = loopbacksync.NewLoopBackSyncInfo(cfg.ChannelID, cfg.LoopbackControl, cfg.SyncDDL)

	var err error
	syncer.tableRouter, syncer.binlogFilter, err = genRouterAndBinlogEvent(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// create schema
	syncer.schema, err = NewSchema(jobs, false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	syncer.dsyncer, err = createDSyncer(cfg, syncer.schema, syncer.loopbackSync, syncer.tableRouter)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return syncer, nil
}

func createDSyncer(cfg *SyncerConfig, schema *Schema, info *loopbacksync.LoopBackSync, tableRouter *router.Table) (dsyncer dsync.Syncer, err error) {
	switch cfg.DestDBType {
	case "kafka":
		dsyncer, err = dsync.NewKafka(cfg.To, schema)
		if err != nil {
			return nil, errors.Annotate(err, "fail to create kafka dsyncer")
		}
	case "file":
		dsyncer, err = dsync.NewPBSyncer(cfg.To.BinlogFileDir, cfg.To.BinlogFileRetentionTime, schema)
		if err != nil {
			return nil, errors.Annotate(err, "fail to create pb dsyncer")
		}
	case "mysql", "tidb", "oracle":
		var relayer relay.Relayer
		if cfg.Relay.IsEnabled() {
			if relayer, err = relay.NewRelayer(cfg.Relay.LogDir, cfg.Relay.MaxFileSize, schema); err != nil {
				return nil, errors.Annotate(err, "fail to create relayer")
			}
		}
		if cfg.DestDBType == "oracle" {
			dsyncer, err = dsync.NewOracleSyncer(cfg.To, schema, cfg.WorkerCount, cfg.TxnBatch, queryHistogramVec, cfg.StrSQLMode, cfg.DestDBType, relayer, cfg.EnableDispatch(), cfg.EnableCausality(), tableRouter)
		} else {
			dsyncer, err = dsync.NewMysqlSyncer(cfg.To, schema, cfg.WorkerCount, cfg.TxnBatch, queryHistogramVec, cfg.StrSQLMode, cfg.DestDBType, relayer, info, cfg.EnableDispatch(), cfg.EnableCausality())
		}
		if err != nil {
			return nil, errors.Annotatef(err, "fail to create %s dsyncer", cfg.DestDBType)
		}
		// only use for test
	case "_intercept":
		dsyncer = newInterceptSyncer()
	default:
		return nil, errors.Errorf("unknown DestDBType: %s", cfg.DestDBType)
	}

	return
}

// Start starts to sync.
func (s *Syncer) Start() error {
	err := s.run()

	return errors.Trace(err)
}

func (s *Syncer) addDMLEventMetrics(muts []pb.TableMutation) {
	for _, mut := range muts {
		for _, tp := range mut.GetSequence() {
			s.addDMLCount(tp, 1)
		}
	}
}

func (s *Syncer) addDMLCount(tp pb.MutationType, nums int) {
	switch tp {
	case pb.MutationType_Insert:
		eventCounter.WithLabelValues("Insert").Add(float64(nums))
	case pb.MutationType_Update:
		eventCounter.WithLabelValues("Update").Add(float64(nums))
	case pb.MutationType_DeleteRow:
		eventCounter.WithLabelValues("Delete").Add(float64(nums))
	}
}

func (s *Syncer) addDDLCount() {
	eventCounter.WithLabelValues("DDL").Add(1)
}

func (s *Syncer) enableSafeModeInitializationPhase() {
	translator.SetSQLMode(s.cfg.SQLMode)

	// for mysql
	// set safeMode to true, it will use the config after 5 minutes.
	if !s.dsyncer.SetSafeMode(true) {
		return
	}

	go func() {
		select {
		case <-time.After(5 * time.Minute):
			s.dsyncer.SetSafeMode(s.cfg.SafeMode)
		case <-s.shutdown:
			return
		}
	}()
}

// handleSuccess handle the success binlog item we synced to downstream,
// currently we only need to save checkpoint ts.
// Note we do not send the fake binlog to downstream, we get fake binlog from
// another chan and it's guaranteed that all the received binlogs before have been synced to downstream
// when we get the fake binlog from this chan.
func (s *Syncer) handleSuccess(fakeBinlog chan *pb.Binlog, lastTS *int64) {
	successes := s.dsyncer.Successes()
	var lastSaveTS int64
	var latestVersion int64
	lastSaveTime := time.Now()

	for {
		if successes == nil && fakeBinlog == nil {
			break
		}

		var (
			saveNow   = false
			appliedTS int64
		)

		select {
		case item, ok := <-successes:
			if !ok {
				successes = nil
				break
			}

			s.lastSyncTime = time.Now()
			ts := item.Binlog.CommitTs
			if ts > atomic.LoadInt64(lastTS) {
				atomic.StoreInt64(lastTS, ts)
			}
			latestVersion = item.SchemaVersion

			// save ASAP for DDL, and if FinishTS > 0, we should save the ts map
			if item.Binlog.DdlJobId > 0 || item.AppliedTS > 0 {
				saveNow = true
				appliedTS = item.AppliedTS
			}

		case binlog, ok := <-fakeBinlog:
			if !ok {
				fakeBinlog = nil
				break
			}
			ts := binlog.CommitTs
			if ts > atomic.LoadInt64(lastTS) {
				atomic.StoreInt64(lastTS, ts)
			}
		}

		ts := atomic.LoadInt64(lastTS)
		if ts > lastSaveTS {
			if saveNow || time.Since(lastSaveTime) > 3*time.Second {
				s.savePoint(ts, appliedTS, latestVersion)
				lastSaveTime = time.Now()
				lastSaveTS = ts
				appliedTS = 0
				eventCounter.WithLabelValues("savepoint").Add(1)
			}
			delay := oracle.GetPhysical(time.Now()) - oracle.ExtractPhysical(uint64(ts))
			checkpointDelayHistogram.Observe(float64(delay) / 1e3)
		}
	}

	ts := atomic.LoadInt64(lastTS)
	if ts > lastSaveTS {
		s.savePoint(ts, 0, latestVersion)
		eventCounter.WithLabelValues("savepoint").Add(1)
	}

	log.Info("handleSuccess quit")
}

func (s *Syncer) savePoint(ts, secondaryTS, version int64) {
	if ts < s.cp.TS() {
		log.Error("save ts is less than checkpoint ts %d", zap.Int64("save ts", ts), zap.Int64("checkpoint ts", s.cp.TS()))
	}

	log.Info("write save point", zap.Int64("ts", ts), zap.Int64("version", version))
	err := s.cp.Save(ts, secondaryTS, false, version)
	if err != nil {
		log.Fatal("save checkpoint failed", zap.Int64("ts", ts), zap.Int64("version", version), zap.Error(err))
	}

	checkpointTSOGauge.Set(float64(oracle.ExtractPhysical(uint64(ts))))
}

func (s *Syncer) run() error {
	defer close(s.closed)

	wait := make(chan struct{})

	fakeBinlogCh := make(chan *pb.Binlog, 1024)
	var lastSuccessTS int64
	var fakeBinlogs []*pb.Binlog
	var fakeBinlogPreAddTS []int64

	go func() {
		defer close(wait)
		s.handleSuccess(fakeBinlogCh, &lastSuccessTS)
	}()

	var err error

	s.enableSafeModeInitializationPhase()

	err = s.schema.handlePreviousDDLJobIfNeed(s.cp.SchemaVersion() + 1)
	if err != nil {
		err = errors.Annotate(err, "handlePreviousDDLJobIfNeed failed")
		return err
	}

	var (
		lastDDLSchemaVersion int64
		b                    *binlogItem
		fakeBinlog           *pb.Binlog
		pushFakeBinlog       chan<- *pb.Binlog
		lastAddCommitTS      int64
		lastFakeCommitTime   time.Time
		p                    = getParser(s.cfg.SQLMode)
	)
	dsyncError := s.dsyncer.Error()

	appendFakeBinlogIfNeeded := func(binlog *pb.Binlog, commitTS int64) {
		if fakeCommitTime := oracle.GetTimeFromTS(uint64(commitTS)); fakeCommitTime.Sub(lastFakeCommitTime) > 3*time.Second {
			lastFakeCommitTime = fakeCommitTime
			if binlog == nil {
				binlog = util.GenFakeBinlog(commitTS)
			}
			fakeBinlogs = append(fakeBinlogs, binlog)
			fakeBinlogPreAddTS = append(fakeBinlogPreAddTS, lastAddCommitTS)
		}
	}

ForLoop:
	for {
		// check if we can safely push a fake binlog
		// We must wait previous items consumed to make sure we are safe to save this fake binlog commitTS
		if pushFakeBinlog == nil && len(fakeBinlogs) > 0 {
			if fakeBinlogPreAddTS[0] <= atomic.LoadInt64(&lastSuccessTS) {
				pushFakeBinlog = fakeBinlogCh
				fakeBinlog = fakeBinlogs[0]
				fakeBinlogs = fakeBinlogs[1:]
				fakeBinlogPreAddTS = fakeBinlogPreAddTS[1:]
			}
		}

		select {
		case err = <-dsyncError:
			break ForLoop
		case <-s.shutdown:
			break ForLoop
		case pushFakeBinlog <- fakeBinlog:
			pushFakeBinlog = nil
			continue
		case b = <-s.input:
			queueSizeGauge.WithLabelValues("syncer_input").Set(float64(len(s.input)))
			log.Debug("consume binlog item", zap.Stringer("item", b))
		}

		binlog := b.binlog
		startTS := binlog.GetStartTs()
		commitTS := binlog.GetCommitTs()
		jobID := binlog.GetDdlJobId()

		if isIgnoreTxnCommitTS(s.cfg.IgnoreTxnCommitTS, commitTS) {
			log.Warn("skip txn", zap.Stringer("binlog", b.binlog))
			continue
		}

		if startTS == commitTS {
			appendFakeBinlogIfNeeded(binlog, commitTS)
		} else if jobID == 0 {
			preWriteValue := binlog.GetPrewriteValue()
			preWrite := &pb.PrewriteValue{}
			err = preWrite.Unmarshal(preWriteValue)
			if err != nil {
				err = errors.Annotatef(err, "prewrite %s Unmarshal failed", preWriteValue)
				break ForLoop
			}

			err = s.rewriteForOldVersion(preWrite)
			if err != nil {
				err = errors.Annotate(err, "rewrite for old version fail")
				break ForLoop
			}

			log.Debug("get DML", zap.Int64("SchemaVersion", preWrite.SchemaVersion))
			if preWrite.SchemaVersion < lastDDLSchemaVersion {
				log.Debug("encounter older schema dml")
			}

			err = s.schema.handlePreviousDDLJobIfNeed(preWrite.SchemaVersion)
			if err != nil {
				err = errors.Annotate(err, "handlePreviousDDLJobIfNeed failed")
				break ForLoop
			}

			var (
				isFilterTransaction = false
				ignore              = false
				err1                error
			)
			if s.loopbackSync != nil && s.loopbackSync.LoopbackControl && s.cfg.DestDBType != "oracle" {
				isFilterTransaction, err1 = loopBackStatus(binlog, preWrite, s.schema, s.loopbackSync)
				if err1 != nil {
					err = errors.Annotate(err1, "analyze transaction failed")
					break ForLoop
				}
			}
			if !isFilterTransaction {
				ignore, err = skipDMLEvent(preWrite, s.schema, s.filter, s.binlogFilter)
				if err != nil {
					err = errors.Annotate(err, "skipDMLEvent failed")
					break ForLoop
				}
			}

			if !ignore && !isFilterTransaction {
				s.addDMLEventMetrics(preWrite.GetMutations())
				beginTime := time.Now()
				lastAddCommitTS = binlog.GetCommitTs()
				err = s.dsyncer.Sync(&dsync.Item{Binlog: binlog, PrewriteValue: preWrite, SchemaVersion: preWrite.SchemaVersion})
				if err != nil {
					err = errors.Annotatef(err, "failed to add item")
					break ForLoop
				}
				executeHistogram.Observe(time.Since(beginTime).Seconds())
			} else {
				log.Debug("skip whole dml event by binlog event filter", zap.Int64("commit ts", commitTS))
				appendFakeBinlogIfNeeded(nil, commitTS)
			}
		} else if jobID > 0 {
			log.Debug("get ddl binlog job", zap.Stringer("job", b.job))

			if skipUnsupportedDDLJob(b.job) {
				log.Info("skip unsupported DDL job", zap.Stringer("job", b.job))
				appendFakeBinlogIfNeeded(nil, commitTS)
				continue
			}

			// Notice: the version of DDL Binlog we receive are Monotonically increasing
			// DDL (with version 10, commit ts 100) -> DDL (with version 9, commit ts 101) would never happen
			s.schema.addJob(b.job)

			log.Debug("get DDL", zap.Int64("SchemaVersion", b.job.BinlogInfo.SchemaVersion))
			lastDDLSchemaVersion = b.job.BinlogInfo.SchemaVersion
			err = s.schema.handlePreviousDDLJobIfNeed(b.job.BinlogInfo.SchemaVersion)
			if err != nil {
				err = errors.Trace(err)
				break ForLoop
			}

			if b.job.SchemaState == model.StateDeleteOnly && b.job.Type == model.ActionDropColumn {
				log.Info("Syncer skips DeleteOnly DDL", zap.Stringer("job", b.job), zap.Int64("ts", b.GetCommitTs()))
				appendFakeBinlogIfNeeded(nil, commitTS)
				continue
			}

			sql := b.job.Query
			var schema, table string
			schema, table, err = s.schema.getSchemaTableAndDelete(b.job.BinlogInfo.SchemaVersion)
			if err != nil {
				err = errors.Trace(err)
				break ForLoop
			}

			if s.filter.SkipSchemaAndTable(schema, table) {
				log.Info("skip ddl by block allow filter", zap.String("schema", schema), zap.String("table", table),
					zap.String("sql", sql), zap.Int64("commit ts", commitTS))
				appendFakeBinlogIfNeeded(nil, commitTS)
				continue
			}

			// shouldSkip is used specially for database dsyncers like tidb/mysql/oracle
			// although we skip some ddls, but we still need to update table info
			// ignore means whether we should should this ddl event after binlogFilter
			var (
				shouldSkip, ignore bool
				stmt               ast.StmtNode
			)

			if stmt, ignore, err = skipDDLEvent(sql, schema, table, p, s.binlogFilter); err != nil {
				break ForLoop
			} else if ignore {
				log.Info("skip ddl by binlog event filter", zap.String("schema", schema), zap.String("table", table),
					zap.String("sql", sql), zap.Int64("commit ts", commitTS))
				// A empty sql force it to evict the downstream table info.
				if s.cfg.DestDBType == "tidb" || s.cfg.DestDBType == "mysql" || s.cfg.DestDBType == "oracle" {
					shouldSkip = true
				} else {
					appendFakeBinlogIfNeeded(nil, commitTS)
					continue
				}
			} else if !ignore && s.cfg.DestDBType == "oracle" {
				if _, ok := stmt.(*ast.TruncateTableStmt); !ok {
					err = errors.Errorf("unsupported ddl %s, you should skip commit ts %d", sql, commitTS)
					break ForLoop
				}
			}

			if !s.cfg.SyncDDL {
				log.Info("skip ddl by SyncDDL setting to false", zap.String("schema", schema), zap.String("table", table),
					zap.String("sql", sql), zap.Int64("commit ts", commitTS))
				// A empty sql force it to evict the downstream table info.
				if s.cfg.DestDBType == "tidb" || s.cfg.DestDBType == "mysql" || s.cfg.DestDBType == "oracle" {
					shouldSkip = true
				} else {
					appendFakeBinlogIfNeeded(nil, commitTS)
					continue
				}
			}

			// Add ddl item to downstream.
			s.addDDLCount()
			beginTime := time.Now()
			lastAddCommitTS = binlog.GetCommitTs()

			log.Info("add ddl item to syncer, you can add this commit ts to `ignore-txn-commit-ts` to skip this ddl if needed",
				zap.String("sql", sql), zap.Int64("commit ts", binlog.CommitTs), zap.Bool("shouldSkip", shouldSkip))

			err = s.dsyncer.Sync(&dsync.Item{Binlog: binlog, PrewriteValue: nil, Schema: schema, Table: table, ShouldSkip: shouldSkip, SchemaVersion: lastDDLSchemaVersion})
			if err != nil {
				err = errors.Annotatef(err, "add to dsyncer, commit ts %d", binlog.CommitTs)
				break ForLoop
			}
			executeHistogram.Observe(time.Since(beginTime).Seconds())
		}
	}

	close(fakeBinlogCh)
	cerr := s.dsyncer.Close()
	if cerr != nil {
		log.Error("Failed to close syncer", zap.Error(cerr))
	}

	select {
	case <-wait:
	case <-time.After(runWaitThreshold):
		panic("Waiting too long for `Syncer.run` to quit.")
	}

	// return the origin error if has, or the close error
	if err != nil {
		return err
	}

	if cerr != nil {
		return cerr
	}

	return s.cp.Save(s.cp.TS(), 0, true /*consistent*/, lastDDLSchemaVersion)
}

func findLoopBackMark(dmls []*loader.DML, info *loopbacksync.LoopBackSync) (bool, error) {
	for _, dml := range dmls {
		tableName := dml.Database + "." + dml.Table
		if strings.EqualFold(tableName, loopbacksync.MarkTableName) {
			channelID, ok := dml.Values[loopbacksync.ChannelID]
			if ok {
				channelIDInt64, ok := channelID.(int64)
				if !ok {
					return false, errors.Errorf("wrong type of channelID: %s", reflect.TypeOf(channelID))
				}
				if channelIDInt64 == info.ChannelID {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func loopBackStatus(binlog *pb.Binlog, prewriteValue *pb.PrewriteValue, infoGetter translator.TableInfoGetter, info *loopbacksync.LoopBackSync) (bool, error) {
	var tableName string
	var schemaName string
	txn, err := translator.TiBinlogToTxn(infoGetter, schemaName, tableName, binlog, prewriteValue, false)
	if err != nil {
		return false, errors.Trace(err)
	}
	return findLoopBackMark(txn.DMLs, info)
}

// skipDMLEvent may drop some table mutation and some sequence in mut in `PrewriteValue`
// Return true if all table mutations are dropped.
func skipDMLEvent(pv *pb.PrewriteValue, schema *Schema, filter *filter.Filter, binlogFilter *bf.BinlogEvent) (ignore bool, err error) {
	var muts []pb.TableMutation
	for _, mutation := range pv.GetMutations() {
		schemaName, tableName, ok := schema.SchemaAndTableName(mutation.GetTableId())
		if !ok {
			return false, errors.Errorf("not found table id: %d", mutation.GetTableId())
		}

		if filter.SkipSchemaAndTable(schemaName, tableName) {
			log.Debug("skip dml", zap.String("schema", schemaName), zap.String("table", tableName))
			continue
		}

		if binlogFilter != nil {
			var (
				insertIdx = 0
				deleteIdx = 0
				updateIdx = 0

				filteredIdx       = 0
				filteredInsertIdx = 0
				filteredDeleteIdx = 0
				filteredUpdateIdx = 0
			)
			for i, tp := range mutation.Sequence {
				var et bf.EventType
				switch tp {
				case binlog.MutationType_Insert:
					et = bf.InsertEvent
					insertIdx++
				case binlog.MutationType_Update:
					et = bf.UpdateEvent
					updateIdx++
				case binlog.MutationType_DeleteRow:
					et = bf.DeleteEvent
					deleteIdx++
				default:
					err = errors.Errorf("unknown mutation type: %v", tp)
					return
				}

				needSkip, err := skipByFilter(binlogFilter, schemaName, tableName, et, "")
				if err != nil {
					return false, errors.Trace(err)
				} else if needSkip {
					continue
				}
				mutation.Sequence[filteredIdx] = mutation.Sequence[i]
				filteredIdx++
				switch tp {
				case binlog.MutationType_Insert:
					mutation.InsertedRows[filteredInsertIdx] = mutation.InsertedRows[insertIdx-1]
					filteredInsertIdx++
				case binlog.MutationType_Update:
					mutation.UpdatedRows[filteredUpdateIdx] = mutation.UpdatedRows[updateIdx-1]
					filteredUpdateIdx++
				case binlog.MutationType_DeleteRow:
					mutation.DeletedRows[filteredDeleteIdx] = mutation.DeletedRows[deleteIdx-1]
					filteredDeleteIdx++
				}
			}
			mutation.Sequence = mutation.Sequence[0:filteredIdx]
			mutation.InsertedRows = mutation.InsertedRows[0:filteredInsertIdx]
			mutation.UpdatedRows = mutation.UpdatedRows[0:filteredUpdateIdx]
			mutation.DeletedRows = mutation.DeletedRows[0:filteredDeleteIdx]
		}

		muts = append(muts, mutation)
	}

	pv.Mutations = muts

	if len(muts) == 0 {
		ignore = true
	}

	return
}

// skipDDLEvent may drop some ddl event
// Return true if this job is filtered.
func skipDDLEvent(sql, schema, table string, p *parser.Parser, binlogFilter *bf.BinlogEvent) (stmt ast.StmtNode, ignore bool, err error) {
	if binlogFilter == nil {
		return nil, false, nil
	}
	stmt, err = p.ParseOneStmt(sql, "", "")
	if err != nil {
		log.L().Error("fail to parse ddl", zap.String("ddl", sql), logutil.ShortError(err))
		// return error if parse fail and filter fail
		needSkip, err2 := skipByFilter(binlogFilter, schema, table, bf.NullEvent, sql)
		return nil, needSkip, err2
	}
	et := bf.AstToDDLEvent(stmt)
	flag, err := skipByFilter(binlogFilter, schema, table, et, sql)
	return stmt, flag, err
}

// skipByFilter returns true when
// * type of SQL doesn't pass binlog-filter.
// * pattern of SQL doesn't pass binlog-filter.
func skipByFilter(binlogFilter *bf.BinlogEvent, schemaName, tableName string, et bf.EventType, sql string) (bool, error) {
	if binlogFilter == nil {
		return false, nil
	}
	action, err := binlogFilter.Filter(schemaName, tableName, et, sql)
	if err != nil {
		return false, errors.Annotatef(err, "fail to skip event (tp: %s, sql: %s) on %v", et, &baf.Table{Schema: schemaName, Name: tableName}, sql)
	}
	return action == bf.Ignore, nil
}

func isIgnoreTxnCommitTS(ignoreTxnCommitTS []int64, ts int64) bool {
	for _, ignoreTS := range ignoreTxnCommitTS {
		if ignoreTS == ts {
			return true
		}
	}
	return false
}

// Add adds binlogItem to the syncer's input channel
func (s *Syncer) Add(b *binlogItem) {
	select {
	case <-s.shutdown:
	case s.input <- b:
		log.Debug("receive publish binlog item", zap.Stringer("item", b))
	}
}

// Close closes syncer.
func (s *Syncer) Close() error {
	log.Debug("closing syncer")
	close(s.shutdown)
	<-s.closed
	log.Debug("syncer is closed")
	return nil
}

// GetLastSyncTime returns lastSyncTime
func (s *Syncer) GetLastSyncTime() time.Time {
	return s.lastSyncTime
}

// GetLatestCommitTS returns the latest commit ts.
func (s *Syncer) GetLatestCommitTS() int64 {
	return s.cp.TS()
}

// see https://github.com/pingcap/tidb/issues/9304
// currently, we only drop the data which table id is truncated.
// because of online DDL, different TiDB instance may see the different schema,
// it can't be treated simply as one timeline consider both DML and DDL,
// we must carefully handle every DDL type now and need to find a better design.
func (s *Syncer) rewriteForOldVersion(pv *pb.PrewriteValue) (err error) {
	var mutations = make([]pb.TableMutation, 0, len(pv.GetMutations()))
	for _, mutation := range pv.GetMutations() {
		if s.schema.IsTruncateTableID(mutation.TableId) {
			log.Info("skip old version truncate dml", zap.Int64("table id", mutation.TableId))
			continue
		}

		mutations = append(mutations, mutation)
	}
	pv.Mutations = mutations

	return nil
}

// interceptSyncer only use for test
type interceptSyncer struct {
	items []*dsync.Item

	successes chan *dsync.Item
	closed    chan struct{}
}

var _ dsync.Syncer = &interceptSyncer{}

func newInterceptSyncer() *interceptSyncer {
	return &interceptSyncer{
		successes: make(chan *dsync.Item, 1024),
		closed:    make(chan struct{}),
	}
}

// SetSafeMode should be ignore by interceptSyncer
func (s *interceptSyncer) SetSafeMode(mode bool) bool {
	return false
}

func (s *interceptSyncer) Sync(item *dsync.Item) error {
	s.items = append(s.items, item)

	s.successes <- item
	return nil
}

func (s *interceptSyncer) Successes() <-chan *dsync.Item {
	return s.successes
}

func (s *interceptSyncer) Close() error {
	close(s.successes)
	close(s.closed)
	return nil
}

func (s *interceptSyncer) Error() <-chan error {
	c := make(chan error, 1)
	go func() {
		<-s.closed
		c <- nil
	}()
	return c
}
