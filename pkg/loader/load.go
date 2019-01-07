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

	// change update -> delete + insert
	// always use replace instead of insert
	safeMode int32

	// always true now
	// merge the same primary key DML sequence, then batch insert
	merge bool
}

// NewLoader return a Loader
// db must support multi statement and interpolateParams
func NewLoader(db *gosql.DB, workerCount int, batchSize int) (*Loader, error) {
	s := &Loader{
		db:          db,
		workerCount: workerCount,
		batchSize:   batchSize,
		input:       make(chan *Txn, 1024),
		successTxn:  make(chan *Txn, 1024),
		merge:       true,
	}

	db.SetMaxOpenConns(workerCount)

	return s, nil
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

// Input return a channel return push Txn into Loader
func (s *Loader) Input() chan<- *Txn {
	return s.input
}

// Successes return a channel to get the successfully Txn loaded to mysql
func (s *Loader) Successes() <-chan *Txn {
	return s.successTxn
}

// Close close the Loader, no more Txn can be push into Input()
// Run will quit when all data is drained
func (s *Loader) Close() error {
	close(s.input)

	return nil
}

func (s *Loader) refreshTableInfo(schema string, table string) (info *tableInfo, err error) {
	info, err = getTableInfo(s.db, schema, table)
	if err != nil {
		return info, errors.Trace(err)
	}

	if len(info.indexs) == 0 {
		log.Warnf("%s has no unique index", quoteSchema(schema, table))
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

		log.Info("exec ddl success")
		return nil
	}

	return errors.Trace(err)
}

// txns should hold only dmls or ddls
func (s *Loader) exec(txns []*Txn) error {
	if len(txns) == 0 {
		return nil
	}

	log.Debug("exec: ", txns)

	var dmls []*DML

	for _, txn := range txns {
		if txn.isDDL() {
			err := s.execDDL(txn.DDL)
			if err != nil {
				if !pkgsql.IgnoreDDLError(err) {
					log.Error(err)
					return errors.Trace(err)
				}
				log.Warnf("ignore ddl error: %v, ddl: %v", err, txn.DDL)
			}

			s.refreshTableInfo(txn.DDL.Database, txn.DDL.Table)
		} else {
			dmls = append(dmls, txn.DMLs...)
		}
	}

	err := s.execDMLs(dmls)
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}

	s.markSuccess(txns...)

	return nil
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
		log.Debug("keys: ", keys)
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

// Run will quit when meet any error, or all the txn are drain
func (s *Loader) Run() error {
	defer func() {
		log.Info("Run()... in Loader quit")
		close(s.successTxn)
	}()

	var err error
	var txns []*Txn
	dmlNumber := 0

	execNow := func() bool {
		return dmlNumber >= s.batchSize*s.workerCount*3
	}

	for {
		select {
		case txn, ok := <-s.input:
			if !ok {
				log.Info("loader closed quit running")
				err = s.exec(txns)
				if err != nil {
					return errors.Trace(err)
				}
				return nil
			}

			if txn.isDDL() {
				err = s.exec(txns)
				if err != nil {
					return errors.Trace(err)
				}
				dmlNumber = 0
				txns = txns[:0]

				err = s.exec([]*Txn{txn})
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				dmlNumber += len(txn.DMLs)
				for _, dml := range txn.DMLs {
					var err error
					dml.info, err = s.getTableInfo(dml.Database, dml.Table)
					if err != nil {
						return errors.Trace(err)
					}
				}
				txns = append(txns, txn)
				if execNow() {
					err = s.exec(txns)
					if err != nil {
						return errors.Trace(err)
					}
					dmlNumber = 0
					txns = txns[:0]
				}
			}
		default:
			if len(txns) > 0 {
				err = s.exec(txns)
				if err != nil {
					return errors.Trace(err)
				}
				dmlNumber = 0
				txns = txns[:0]
				continue
			}

			// get first
			txn, ok := <-s.input
			if !ok {
				return nil
			}
			if txn.isDDL() {
				err = s.exec([]*Txn{txn})
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				dmlNumber += len(txn.DMLs)
				for _, dml := range txn.DMLs {
					var err error
					dml.info, err = s.getTableInfo(dml.Database, dml.Table)
					if err != nil {
						return errors.Trace(err)
					}
				}
				txns = append(txns, txn)
			}
		}
	}
}
