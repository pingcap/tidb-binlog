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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	dsync "github.com/pingcap/tidb-binlog/drainer/sync"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/filter"
	"github.com/pingcap/tidb/store/tikv/oracle"
	pb "github.com/pingcap/tipb/go-binlog"
)

// Syncer converts tidb binlog to the specified DB sqls, and sync it to target DB
type Syncer struct {
	schema *Schema
	cp     checkpoint.CheckPoint

	cfg *SyncerConfig

	input chan *binlogItem

	filter *filter.Filter

	// last time we successfully sync binlog item to downstream
	lastSyncTime time.Time

	dsyncer dsync.Syncer
	itemsWg sync.WaitGroup

	shutdown chan struct{}
	closed   chan struct{}
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

	var err error
	// create schema
	syncer.schema, err = NewSchema(jobs, false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	syncer.dsyncer, err = createDSyncer(cfg, syncer.schema)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return syncer, nil
}

func createDSyncer(cfg *SyncerConfig, schema *Schema) (dsyncer dsync.Syncer, err error) {
	switch cfg.DestDBType {
	case "kafka":
		dsyncer, err = dsync.NewKafka(cfg.To, schema)
		if err != nil {
			return nil, errors.Annotate(err, "fail to create kafka dsyncer")
		}
	case "file":
		dsyncer, err = dsync.NewPBSyncer(cfg.To.BinlogFileDir, schema)
		if err != nil {
			return nil, errors.Annotate(err, "fail to create pb dsyncer")
		}
	case "flash":
		dsyncer, err = dsync.NewFlashSyncer(cfg.To, schema)
		if err != nil {
			return nil, errors.Annotate(err, "fail to create flash dsyncer")
		}
	case "mysql", "tidb":
		dsyncer, err = dsync.NewMysqlSyncer(cfg.To, schema, cfg.WorkerCount, cfg.TxnBatch, queryHistogramVec, cfg.StrSQLMode)
		if err != nil {
			return nil, errors.Annotate(err, "fail to create mysql dsyncer")
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
	// set safeMode to true at the first, and will use the config after 5 minutes.
	mysqlSyncer, ok := s.dsyncer.(*dsync.MysqlSyncer)
	if !ok {
		return
	}

	mysqlSyncer.SetSafeMode(true)

	go func() {
		select {
		case <-time.After(5 * time.Minute):
			mysqlSyncer.SetSafeMode(s.cfg.SafeMode)
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

	for {
		if successes == nil && fakeBinlog == nil {
			break
		}

		var saveNow = false

		select {
		case item, ok := <-successes:
			if !ok {
				successes = nil
				break
			}

			s.lastSyncTime = time.Now()
			s.itemsWg.Done()
			ts := item.Binlog.CommitTs
			if ts > atomic.LoadInt64(lastTS) {
				atomic.StoreInt64(lastTS, ts)
			}

			// save ASAP for DDL
			if item.Binlog.DdlJobId > 0 {
				saveNow = true
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
			if saveNow || s.cp.Check(ts) {
				s.savePoint(ts)
				lastSaveTS = ts
				eventCounter.WithLabelValues("savepoint").Add(1)
			}
			delay := oracle.GetPhysical(time.Now()) - oracle.ExtractPhysical(uint64(ts))
			checkpointDelayGauge.Set(float64(delay) / 1e3)
		}
	}

	ts := atomic.LoadInt64(lastTS)
	if ts > lastSaveTS {
		s.savePoint(ts)
		eventCounter.WithLabelValues("savepoint").Add(1)
	}

	log.Info("handleSuccess quit")
}

func (s *Syncer) savePoint(ts int64) {
	if ts < s.cp.TS() {
		log.Error("save ts is less than checkpoint ts %d", zap.Int64("save ts", ts), zap.Int64("checkpoint ts", s.cp.TS()))
	}

	log.Info("write save point", zap.Int64("ts", ts))
	err := s.cp.Save(ts)
	if err != nil {
		log.Fatal("save checkpoint failed", zap.Int64("ts", ts), zap.Error(err))
	}

	checkpointTSOGauge.Set(float64(oracle.ExtractPhysical(uint64(ts))))
}

func (s *Syncer) run() error {
	var wg sync.WaitGroup

	fakeBinlogCh := make(chan *pb.Binlog, 1024)
	var lastSuccessTS int64
	var fakeBinlogs []*pb.Binlog
	var fakeBinlogPreAddTS []int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.handleSuccess(fakeBinlogCh, &lastSuccessTS)
	}()

	var err error

	s.enableSafeModeInitializationPhase()

	var lastDDLSchemaVersion int64
	var b *binlogItem

	var fakeBinlog *pb.Binlog
	var pushFakeBinlog chan<- *pb.Binlog = nil

	var lastAddComitTS int64
	dsyncError := s.dsyncer.Error()
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

		if startTS == commitTS {
			fakeBinlogs = append(fakeBinlogs, binlog)
			fakeBinlogPreAddTS = append(fakeBinlogPreAddTS, lastAddComitTS)
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

			var ignore bool
			ignore, err = filterTable(preWrite, s.filter, s.schema)
			if err != nil {
				err = errors.Annotate(err, "filterTable failed")
				break ForLoop
			}

			if !ignore {
				s.addDMLEventMetrics(preWrite.GetMutations())
				beginTime := time.Now()
				s.itemsWg.Add(1)
				lastAddComitTS = binlog.GetCommitTs()
				err = s.dsyncer.Sync(&dsync.Item{Binlog: binlog, PrewriteValue: preWrite})
				if err != nil {
					err = errors.Annotate(err, "add to dsyncer failed")
					break ForLoop
				}
				executeHistogram.Observe(time.Since(beginTime).Seconds())
			}
		} else if jobID > 0 {
			log.Debug("get ddl binlog job", zap.Stringer("job", b.job))

			// Notice: the version of DDL Binlog we receive are Monotonically increasing
			// DDL (with version 10, commit ts 100) -> DDL (with version 9, commit ts 101) would never happen
			s.schema.addJob(b.job)

			log.Debug("get DDL", zap.Int64("SchemaVersion", b.job.BinlogInfo.SchemaVersion))
			lastDDLSchemaVersion = b.job.BinlogInfo.SchemaVersion

			err = s.schema.handlePreviousDDLJobIfNeed(b.job.BinlogInfo.SchemaVersion)
			if err != nil {
				return errors.Trace(err)
			}

			sql := b.job.Query
			var schema, table string
			schema, table, err = s.schema.getSchemaTableAndDelete(b.job.BinlogInfo.SchemaVersion)
			if err != nil {
				return errors.Trace(err)
			}

			if s.filter.SkipSchemaAndTable(schema, table) {
				log.Info("skip ddl", zap.String("schema", schema), zap.String("table", table),
					zap.String("sql", sql), zap.Int64("commit ts", commitTS))
			} else if sql != "" {
				s.addDDLCount()
				beginTime := time.Now()
				s.itemsWg.Add(1)
				lastAddComitTS = binlog.GetCommitTs()
				err = s.dsyncer.Sync(&dsync.Item{Binlog: binlog, PrewriteValue: nil, Schema: schema, Table: table})
				if err != nil {
					err = errors.Annotate(err, "add to dsyncer failed")
					break ForLoop
				}
				executeHistogram.Observe(time.Since(beginTime).Seconds())
			}
		}
	}

	close(fakeBinlogCh)
	cerr := s.dsyncer.Close()
	wg.Wait()

	close(s.closed)

	// return the origin error if has, or the close error
	if err != nil {
		return err
	}
	return cerr
}

// filterTable may drop some table mutation in `PrewriteValue`
// Return true if all table mutations are dropped.
func filterTable(pv *pb.PrewriteValue, filter *filter.Filter, schema *Schema) (ignore bool, err error) {
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

		muts = append(muts, mutation)
	}

	pv.Mutations = muts

	if len(muts) == 0 {
		ignore = true
	}

	return
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
