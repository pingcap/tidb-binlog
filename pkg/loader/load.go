package loader

import (
	"context"
	gosql "database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
)

const (
	maxDMLRetryCount = 100
	maxDDLRetryCount = 5
)

// Loader is used to load data to mysql
type Loader struct {
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
}

// MetricsGroup contains metrics of Loader
type MetricsGroup struct {
	EventCounterVec   *prometheus.CounterVec
	QueryHistogramVec *prometheus.HistogramVec
}

type options struct {
	workerCount int
	batchSize   int
	metrics     *MetricsGroup
}

var defaultLoaderOptions = options{
	workerCount: 16,
	batchSize:   20,
	metrics:     nil,
}

// A LoaderOption sets options such batch size, worker count etc.
type LoaderOption func(*options)

// WorkerCount set worker count of loader
func WorkerCount(n int) LoaderOption {
	return func(o *options) {
		o.workerCount = n
	}
}

// BatchSize set batch size of loader
func BatchSize(n int) LoaderOption {
	return func(o *options) {
		o.batchSize = n
	}
}

// Metrics set metrics of loader
func Metrics(m *MetricsGroup) LoaderOption {
	return func(o *options) {
		o.metrics = m
	}
}

// NewLoader return a Loader
// db must support multi statement and interpolateParams
func NewLoader(db *gosql.DB, opt ...LoaderOption) (*Loader, error) {
	opts := defaultLoaderOptions
	for _, o := range opt {
		o(&opts)
	}

	s := &Loader{
		db:          db,
		workerCount: opts.workerCount,
		batchSize:   opts.batchSize,
		metrics:     opts.metrics,
		input:       make(chan *Txn, 1024),
		successTxn:  make(chan *Txn, 1024),
		merge:       true,
	}

	db.SetMaxOpenConns(opts.workerCount)

	return s, nil
}

func (s *Loader) metricsInputTxn(txn *Txn) {
	if s.metrics == nil {
		return
	}

	s.metrics.EventCounterVec.WithLabelValues("Txn").Add(1)

	if txn.isDDL() {
		s.metrics.EventCounterVec.WithLabelValues("DDL").Add(1)
	} else {
		var insertEvent float64
		var deleteEvent float64
		var updateEvent float64
		for _, dml := range txn.DMLs {
			switch dml.Tp {
			case InsertDMLType:
				insertEvent++
			case UpdateDMLType:
				updateEvent++
			case DeleteDMLType:
				deleteEvent++
			}
		}
		s.metrics.EventCounterVec.WithLabelValues("Insert").Add(insertEvent)
		s.metrics.EventCounterVec.WithLabelValues("Update").Add(updateEvent)
		s.metrics.EventCounterVec.WithLabelValues("Delete").Add(deleteEvent)
	}
}

// SetSafeMode set safe mode
func (s *Loader) SetSafeMode(safe bool) {
	if safe {
		atomic.StoreInt32(&s.safeMode, 1)
	} else {
		atomic.StoreInt32(&s.safeMode, 0)
	}
}

// GetSafeMode get safe mode
func (s *Loader) GetSafeMode() bool {
	v := atomic.LoadInt32(&s.safeMode)

	return v != 0
}

func (s *Loader) markSuccess(txns ...*Txn) {
	log.Debug("markSuccess: ", txns)
	for _, txn := range txns {
		s.successTxn <- txn
	}
}

// Input returns input channel which used to put Txn into Loader
func (s *Loader) Input() chan<- *Txn {
	return s.input
}

// Successes return a channel to get the successfully Txn loaded to mysql
func (s *Loader) Successes() <-chan *Txn {
	return s.successTxn
}

// Close close the Loader, no more Txn can be push into Input()
// Run will quit when all data is drained
func (s *Loader) Close() {
	close(s.input)
}

func (s *Loader) refreshTableInfo(schema string, table string) (info *tableInfo, err error) {
	info, err = getTableInfo(s.db, schema, table)
	if err != nil {
		return info, errors.Trace(err)
	}

	if len(info.uniqueKeys) == 0 {
		log.Warnf("table %s has no any primary key and unique index, it may be slow when syncing data to downstream, we highly recommend add primary key for table", quoteSchema(schema, table))
	}

	s.tableInfos.Store(quoteSchema(schema, table), info)

	return
}

func (s *Loader) getTableInfo(schema string, table string) (info *tableInfo, err error) {
	v, ok := s.tableInfos.Load(quoteSchema(schema, table))
	if ok {
		info = v.(*tableInfo)
		return
	}

	return s.refreshTableInfo(schema, table)
}

func (s *Loader) execDDL(ddl *DDL) error {
	log.Debug("exec ddl: ", ddl)
	var err error
	var tx *gosql.Tx
	for i := 0; i < maxDDLRetryCount; i++ {
		if i > 0 {
			time.Sleep(time.Second)
		}

		tx, err = s.db.Begin()
		if err != nil {
			log.Error(err)
			continue
		}

		if len(ddl.Database) > 0 {
			_, err = tx.Exec(fmt.Sprintf("use %s;", ddl.Database))
			if err != nil {
				log.Error(err)
				tx.Rollback()
				continue
			}
		}

		log.Infof("retry num: %d, exec ddl: %s", i, ddl.SQL)
		_, err = tx.Exec(ddl.SQL)
		if err != nil {
			log.Error(err)
			tx.Rollback()
			continue
		}

		err = tx.Commit()
		if err != nil {
			log.Error(err)
			continue
		}

		log.Info("exec ddl success: ", ddl.SQL)
		return nil
	}

	return errors.Trace(err)
}

func (s *Loader) execByHash(executor *executor, byHash [][]*DML) error {
	errg, _ := errgroup.WithContext(context.Background())

	for _, dmls := range byHash {
		if len(dmls) == 0 {
			continue
		}

		dmls := dmls

		errg.Go(func() error {
			err := executor.singleExecRetry(dmls, s.GetSafeMode(), maxDMLRetryCount, time.Second)
			return err
		})
	}

	err := errg.Wait()

	return errors.Trace(err)
}

func (s *Loader) singleExec(executor *executor, dmls []*DML) error {
	causality := NewCausality()

	var byHash = make([][]*DML, s.workerCount)

	for _, dml := range dmls {
		keys := getKeys(dml)
		log.Debugf("dml: %v keys: %v", dml, keys)
		conflict := causality.DetectConflict(keys)
		if conflict {
			log.Info("causality.DetectConflict")
			err := s.execByHash(executor, byHash)
			if err != nil {
				return errors.Trace(err)
			}

			causality.Reset()
			for i := 0; i < len(byHash); i++ {
				byHash[i] = byHash[i][:0]
			}
		} else {
			causality.Add(keys)
			key := causality.Get(keys[0])
			idx := int(genHashKey(key)) % len(byHash)
			byHash[idx] = append(byHash[idx], dml)
		}

	}

	err := s.execByHash(executor, byHash)
	return errors.Trace(err)
}

func (s *Loader) execDMLs(dmls []*DML) error {
	if len(dmls) == 0 {
		return nil
	}

	log.Debug("exec dml: ", dmls)

	for _, dml := range dmls {
		var err error
		dml.info, err = s.getTableInfo(dml.Database, dml.Table)
		if err != nil {
			return errors.Trace(err)
		}
	}

	tables := groupByTable(dmls)

	batchTables := make(map[string][]*DML)
	var singleDMLs []*DML

	for tableName, tableDMLs := range tables {
		if len(tableDMLs[0].primaryKeys()) > 0 && s.merge {
			batchTables[tableName] = tableDMLs
		} else {
			singleDMLs = append(singleDMLs, tableDMLs...)
		}
	}

	log.Debugf("exec by tables: %v by single: %v", batchTables, singleDMLs)

	errg, _ := errgroup.WithContext(context.Background())
	executor := newExecutor(s.db).withBatchSize(s.batchSize)
	if s.metrics != nil {
		executor = executor.withQueryHistogramVec(s.metrics.QueryHistogramVec)
	}

	for _, dmls := range batchTables {
		// https://golang.org/doc/faq#closures_and_goroutines
		dmls := dmls
		errg.Go(func() error {
			err := executor.execTableBatchRetry(dmls, maxDMLRetryCount, time.Second)
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
func (s *Loader) Run() error {
	defer func() {
		log.Info("Run()... in Loader quit")
		close(s.successTxn)
	}()

	var err error

	// the txns and according dmls we accumulate to execute later
	var txns []*Txn
	var dmls []*DML

	execDML := func() error {
		err := s.execDMLs(dmls)
		if err != nil {
			return errors.Trace(err)
		}

		s.markSuccess(txns...)
		txns = txns[:0]
		dmls = dmls[:0]
		return nil
	}

	execDDL := func(txn *Txn) error {
		err := s.execDDL(txn.DDL)
		if err != nil {
			if !pkgsql.IgnoreDDLError(err) {
				log.Errorf("exe ddl: %s fail: %v", txn.DDL.SQL, err)
				return errors.Trace(err)
			}
			log.Warnf("ignore ddl error: %v, ddl: %v", err, txn.DDL)
		}

		s.markSuccess(txn)
		s.refreshTableInfo(txn.DDL.Database, txn.DDL.Table)
		return nil
	}

	handleTxn := func(txn *Txn) error {
		s.metricsInputTxn(txn)

		// we always executor the previous dmls when we meet ddl,
		// and executor ddl one by one.
		if txn.isDDL() {
			if err = execDML(); err != nil {
				return errors.Trace(err)
			}

			err = execDDL(txn)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			dmls = append(dmls, txn.DMLs...)
			txns = append(txns, txn)

			// reach a limit size to exec
			if len(dmls) >= s.batchSize*s.workerCount*3 {
				if err = execDML(); err != nil {
					return errors.Trace(err)
				}
			}
		}

		return nil
	}

	for {
		select {
		case txn, ok := <-s.input:
			if !ok {
				log.Info("loader closed quit running")
				if err = execDML(); err != nil {
					return errors.Trace(err)
				}
				return nil
			}

			if err = handleTxn(txn); err != nil {
				return errors.Trace(err)
			}

		default:
			// excute dmls ASAP if no more txn we can get
			if len(dmls) > 0 {
				if err = execDML(); err != nil {
					return errors.Trace(err)
				}

				continue
			}

			// get first
			txn, ok := <-s.input
			if !ok {
				return nil
			}

			if err = handleTxn(txn); err != nil {
				return errors.Trace(err)
			}
		}
	}
}
