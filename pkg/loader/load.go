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

package loader

import (
	"context"
	gosql "database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	tmysql "github.com/pingcap/parser/mysql"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
)

const (
	maxDMLRetryCount = 100
	maxDDLRetryCount = 5

	execLimitMultiple = 3
)

var (
	execDDLRetryWait            = time.Second
	fNewBatchManager            = newBatchManager
	fGetAppliedTS               = getAppliedTS
	updateLastAppliedTSInterval = time.Minute
)

// Loader is used to load data to mysql
type Loader interface {
	SetSafeMode(bool)
	GetSafeMode() bool
	Input() chan<- *Txn
	Successes() <-chan *Txn
	Close()
	Run() error
}

var _ Loader = &loaderImpl{}

type loaderImpl struct {
	// we can get table info from downstream db
	// like column name, pk & uk
	db *gosql.DB

	tableInfos sync.Map

	batchSize   int
	workerCount int

	input      chan *Txn
	successTxn chan *Txn

	metrics *MetricsGroup

	// change update -> delete + replace
	// insert -> replace
	safeMode int32

	// always true now
	// merge the same primary key DML sequence, then batch insert
	merge bool

	// value can be tidb or mysql
	saveAppliedTS           bool
	lastUpdateAppliedTSTime time.Time

	// TODO: remove this ctx, context shouldn't stored in struct
	// https://github.com/pingcap/tidb-binlog/pull/691#issuecomment-515387824
	ctx    context.Context
	cancel context.CancelFunc
}

// MetricsGroup contains metrics of Loader
type MetricsGroup struct {
	EventCounterVec   *prometheus.CounterVec
	QueryHistogramVec *prometheus.HistogramVec
}

type options struct {
	workerCount   int
	batchSize     int
	metrics       *MetricsGroup
	saveAppliedTS bool
}

var defaultLoaderOptions = options{
	workerCount:   16,
	batchSize:     20,
	metrics:       nil,
	saveAppliedTS: false,
}

// A Option sets options such batch size, worker count etc.
type Option func(*options)

// WorkerCount set worker count of loader
func WorkerCount(n int) Option {
	return func(o *options) {
		o.workerCount = n
	}
}

// BatchSize set batch size of loader
func BatchSize(n int) Option {
	return func(o *options) {
		o.batchSize = n
	}
}

// SaveAppliedTS set downstream type, values can be tidb or mysql
func SaveAppliedTS(save bool) Option {
	return func(o *options) {
		o.saveAppliedTS = save
	}
}

// Metrics set metrics of loader
func Metrics(m *MetricsGroup) Option {
	return func(o *options) {
		o.metrics = m
	}
}

// NewLoader return a Loader
// db must support multi statement and interpolateParams
func NewLoader(db *gosql.DB, opt ...Option) (Loader, error) {
	opts := defaultLoaderOptions
	for _, o := range opt {
		o(&opts)
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &loaderImpl{
		db:            db,
		workerCount:   opts.workerCount,
		batchSize:     opts.batchSize,
		metrics:       opts.metrics,
		input:         make(chan *Txn, 1024),
		successTxn:    make(chan *Txn, 1024),
		merge:         true,
		saveAppliedTS: opts.saveAppliedTS,

		ctx:    ctx,
		cancel: cancel,
	}

	db.SetMaxOpenConns(opts.workerCount)
	db.SetMaxIdleConns(opts.workerCount)

	return s, nil
}

func (s *loaderImpl) metricsInputTxn(txn *Txn) {
	if s.metrics == nil || s.metrics.EventCounterVec == nil {
		return
	}

	s.metrics.EventCounterVec.WithLabelValues("Txn").Inc()

	if txn.isDDL() {
		s.metrics.EventCounterVec.WithLabelValues("DDL").Add(1)
	} else {
		nInsert, nDelete, nUpdate := countEvents(txn.DMLs)
		s.metrics.EventCounterVec.WithLabelValues("Insert").Add(nInsert)
		s.metrics.EventCounterVec.WithLabelValues("Delete").Add(nDelete)
		s.metrics.EventCounterVec.WithLabelValues("Update").Add(nUpdate)
	}
}

// SetSafeMode set safe mode
func (s *loaderImpl) SetSafeMode(safe bool) {
	if safe {
		atomic.StoreInt32(&s.safeMode, 1)
	} else {
		atomic.StoreInt32(&s.safeMode, 0)
	}
}

// GetSafeMode get safe mode
func (s *loaderImpl) GetSafeMode() bool {
	v := atomic.LoadInt32(&s.safeMode)

	return v != 0
}

func (s *loaderImpl) markSuccess(txns ...*Txn) {
	if s.saveAppliedTS && len(txns) > 0 && time.Since(s.lastUpdateAppliedTSTime) > updateLastAppliedTSInterval {
		txns[len(txns)-1].AppliedTS = fGetAppliedTS(s.db)
		s.lastUpdateAppliedTSTime = time.Now()
	}
	for _, txn := range txns {
		s.successTxn <- txn
	}
	log.Debug("markSuccess txns", zap.Int("txns len", len(txns)))
}

// Input returns input channel which used to put Txn into Loader
func (s *loaderImpl) Input() chan<- *Txn {
	return s.input
}

// Successes return a channel to get the successfully Txn loaded to mysql
func (s *loaderImpl) Successes() <-chan *Txn {
	return s.successTxn
}

// Close close the Loader, no more Txn can be push into Input()
// Run will quit when all data is drained
func (s *loaderImpl) Close() {
	close(s.input)
	s.cancel()
}

var utilGetTableInfo = getTableInfo

func (s *loaderImpl) refreshTableInfo(schema string, table string) (info *tableInfo, err error) {
	log.Info("refresh table info", zap.String("schema", schema), zap.String("table", table))

	if len(schema) == 0 {
		return nil, errors.New("schema is empty")
	}

	if len(table) == 0 {
		return nil, nil
	}

	info, err = utilGetTableInfo(s.db, schema, table)
	if err != nil {
		return info, errors.Trace(err)
	}

	if len(info.uniqueKeys) == 0 {
		log.Warn("table has no any primary key and unique index, it may be slow when syncing data to downstream, we highly recommend add primary key or unique key for table", zap.String("table", quoteSchema(schema, table)))
	}

	s.tableInfos.Store(quoteSchema(schema, table), info)

	return
}

func (s *loaderImpl) getTableInfo(schema string, table string) (info *tableInfo, err error) {
	v, ok := s.tableInfos.Load(quoteSchema(schema, table))
	if ok {
		info = v.(*tableInfo)
		return
	}

	return s.refreshTableInfo(schema, table)
}

func needRefreshTableInfo(sql string) bool {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		log.Error("parse sql failed", zap.String("sql", sql), zap.Error(err))
		return true
	}

	switch stmt.(type) {
	case *ast.DropTableStmt:
		return false
	case *ast.DropDatabaseStmt:
		return false
	case *ast.TruncateTableStmt:
		return false
	case *ast.CreateDatabaseStmt:
		return false
	}

	return true
}

func isCreateDatabaseDDL(sql string) bool {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		log.Error("parse sql failed", zap.String("sql", sql), zap.Error(err))
		return false
	}

	_, isCreateDatabase := stmt.(*ast.CreateDatabaseStmt)
	return isCreateDatabase
}

func (s *loaderImpl) execDDL(ddl *DDL) error {
	log.Debug("exec ddl", zap.Reflect("ddl", ddl))

	err := util.RetryContext(s.ctx, maxDDLRetryCount, execDDLRetryWait, 1, func(context.Context) error {
		tx, err := s.db.Begin()
		if err != nil {
			return err
		}

		if len(ddl.Database) > 0 && !isCreateDatabaseDDL(ddl.SQL) {
			_, err = tx.Exec(fmt.Sprintf("use %s;", quoteName(ddl.Database)))
			if err != nil {
				tx.Rollback()
				return err
			}
		}

		if _, err = tx.Exec(ddl.SQL); err != nil {
			tx.Rollback()
			return err
		}

		if err = tx.Commit(); err != nil {
			return err
		}

		log.Info("exec ddl success", zap.String("sql", ddl.SQL))
		return nil
	})

	return errors.Trace(err)
}

func (s *loaderImpl) execByHash(executor *executor, byHash [][]*DML) error {
	errg, _ := errgroup.WithContext(s.ctx)

	for _, dmls := range byHash {
		if len(dmls) == 0 {
			continue
		}

		dmls := dmls

		errg.Go(func() error {
			err := executor.singleExecRetry(s.ctx, dmls, s.GetSafeMode(), maxDMLRetryCount, time.Second)
			return err
		})
	}

	err := errg.Wait()

	return errors.Trace(err)
}

func (s *loaderImpl) singleExec(executor *executor, dmls []*DML) error {
	causality := NewCausality()

	var byHash = make([][]*DML, s.workerCount)

	for _, dml := range dmls {
		keys := getKeys(dml)
		log.Debug("get keys", zap.Reflect("dml", dml), zap.Strings("keys", keys))
		conflict := causality.DetectConflict(keys)
		if conflict {
			log.Info("meet causality.DetectConflict exec now",
				zap.String("table name", dml.TableName()),
				zap.Strings("keys", keys))
			if err := s.execByHash(executor, byHash); err != nil {
				return errors.Trace(err)
			}

			causality.Reset()
			for i := 0; i < len(byHash); i++ {
				byHash[i] = byHash[i][:0]
			}
		}

		causality.Add(keys)
		key := causality.Get(keys[0])
		idx := int(genHashKey(key)) % len(byHash)
		byHash[idx] = append(byHash[idx], dml)

	}

	err := s.execByHash(executor, byHash)
	return errors.Trace(err)
}

func (s *loaderImpl) execDMLs(dmls []*DML) error {
	if len(dmls) == 0 {
		return nil
	}

	for _, dml := range dmls {
		if err := s.setDMLInfo(dml); err != nil {
			return errors.Trace(err)
		}
		filterGeneratedCols(dml)
	}

	batchTables, singleDMLs := s.groupDMLs(dmls)

	executor := s.getExecutor()
	errg, _ := errgroup.WithContext(s.ctx)

	for _, dmls := range batchTables {
		// https://golang.org/doc/faq#closures_and_goroutines
		dmls := dmls
		errg.Go(func() error {
			err := executor.execTableBatchRetry(s.ctx, dmls, maxDMLRetryCount, time.Second)
			return err
		})
	}

	errg.Go(func() error {
		err := s.singleExec(executor, singleDMLs)
		return errors.Trace(err)
	})

	err := errg.Wait()

	return errors.Trace(err)
}

// Run will quit when meet any error, or all the txn are drained
func (s *loaderImpl) Run() error {
	defer func() {
		log.Info("Run()... in Loader quit")
		close(s.successTxn)
	}()

	batch := fNewBatchManager(s)

	for {
		select {
		case txn, ok := <-s.input:
			if !ok {
				log.Info("Loader closed, quit running")
				if err := batch.execAccumulatedDMLs(); err != nil {
					return errors.Trace(err)
				}
				return nil
			}

			s.metricsInputTxn(txn)
			if err := batch.put(txn); err != nil {
				return errors.Trace(err)
			}

		default:
			// execute DMLs ASAP if the `input` channel is empty
			if len(batch.dmls) > 0 {
				if err := batch.execAccumulatedDMLs(); err != nil {
					return errors.Trace(err)
				}

				continue
			}

			// get first
			txn, ok := <-s.input
			if !ok {
				return nil
			}

			s.metricsInputTxn(txn)
			if err := batch.put(txn); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// groupDMLs group DMLs by table in batchByTbls and
// collects DMLs that can't be executed in bulk in singleDMLs.
// NOTE: DML.info are assumed to be already set.
func (s *loaderImpl) groupDMLs(dmls []*DML) (batchByTbls map[string][]*DML, singleDMLs []*DML) {
	if !s.merge {
		singleDMLs = dmls
		return
	}
	batchByTbls = make(map[string][]*DML)
	for _, dml := range dmls {
		info := dml.info
		if info.primaryKey != nil && len(info.uniqueKeys) == 0 {
			tblName := dml.TableName()
			batchByTbls[tblName] = append(batchByTbls[tblName], dml)
		} else {
			singleDMLs = append(singleDMLs, dml)
		}
	}
	return
}

func countEvents(dmls []*DML) (insertEvent float64, deleteEvent float64, updateEvent float64) {
	for _, dml := range dmls {
		switch dml.Tp {
		case InsertDMLType:
			insertEvent++
		case UpdateDMLType:
			updateEvent++
		case DeleteDMLType:
			deleteEvent++
		}
	}
	return
}

func (s *loaderImpl) setDMLInfo(dml *DML) (err error) {
	dml.info, err = s.getTableInfo(dml.Database, dml.Table)
	if err != nil {
		err = errors.Trace(err)
	}
	return
}

func filterGeneratedCols(dml *DML) {
	if len(dml.Values) > len(dml.info.columns) {
		// Remove values of generated columns
		vals := make(map[string]interface{}, len(dml.info.columns))
		for _, col := range dml.info.columns {
			vals[col] = dml.Values[col]
		}
		dml.Values = vals
	}
}

func (s *loaderImpl) getExecutor() *executor {
	e := newExecutor(s.db).withBatchSize(s.batchSize)
	if s.metrics != nil && s.metrics.QueryHistogramVec != nil {
		e = e.withQueryHistogramVec(s.metrics.QueryHistogramVec)
	}
	return e
}

func newBatchManager(s *loaderImpl) *batchManager {
	return &batchManager{
		limit:                s.batchSize * s.workerCount * execLimitMultiple,
		fExecDMLs:            s.execDMLs,
		fDMLsSuccessCallback: s.markSuccess,
		fExecDDL:             s.execDDL,
		fDDLSuccessCallback: func(txn *Txn) {
			s.markSuccess(txn)
			if needRefreshTableInfo(txn.DDL.SQL) {
				if _, err := s.refreshTableInfo(txn.DDL.Database, txn.DDL.Table); err != nil {
					log.Error("refresh table info failed", zap.String("database", txn.DDL.Database), zap.String("table", txn.DDL.Table), zap.Error(err))
				}
			}
		},
	}
}

type batchManager struct {
	txns                 []*Txn
	dmls                 []*DML
	limit                int
	fExecDMLs            func([]*DML) error
	fDMLsSuccessCallback func(...*Txn)
	fExecDDL             func(*DDL) error
	fDDLSuccessCallback  func(*Txn)
}

func (b *batchManager) execAccumulatedDMLs() (err error) {
	if len(b.dmls) == 0 {
		return nil
	}

	if err := b.fExecDMLs(b.dmls); err != nil {
		return errors.Trace(err)
	}

	if b.fDMLsSuccessCallback != nil {
		b.fDMLsSuccessCallback(b.txns...)
	}
	b.txns = b.txns[:0]
	b.dmls = b.dmls[:0]
	return nil
}

func (b *batchManager) execDDL(txn *Txn) error {
	if err := b.fExecDDL(txn.DDL); err != nil {
		if !pkgsql.IgnoreDDLError(err) {
			log.Error("exec failed", zap.String("sql", txn.DDL.SQL), zap.Error(err))
			return errors.Trace(err)
		}
		log.Warn("ignore ddl", zap.Error(err), zap.String("ddl", txn.DDL.SQL))
	}

	b.fDDLSuccessCallback(txn)
	return nil
}

func (b *batchManager) put(txn *Txn) error {
	// we always executor the previous dmls when we meet ddl,
	// and executor ddl one by one.
	if txn.isDDL() {
		if len(txn.DDL.Database) == 0 {
			return errors.Errorf("get DDL Txn with empty database, ddl: %s", txn.DDL.SQL)
		}

		if err := b.execAccumulatedDMLs(); err != nil {
			return errors.Trace(err)
		}
		if err := b.execDDL(txn); err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	b.dmls = append(b.dmls, txn.DMLs...)
	b.txns = append(b.txns, txn)

	// reach a limit size to exec
	if len(b.dmls) >= b.limit {
		if err := b.execAccumulatedDMLs(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func getAppliedTS(db *gosql.DB) int64 {
	appliedTS, err := pkgsql.GetTidbPosition(db)
	if err != nil {
		errCode, ok := pkgsql.GetSQLErrCode(err)
		// if tidb dont't support `show master status`, will return 1105 ErrUnknown error
		if !ok || int(errCode) != tmysql.ErrUnknown {
			log.Warn("get ts from slave cluster failed", zap.Error(err))
		}
		return 0
	}
	return appliedTS
}
