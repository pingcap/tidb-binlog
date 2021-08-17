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

package sync

import (
	"context"
	"database/sql"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

var _ Syncer = &MysqlSyncer{}

// MysqlSyncer sync binlog to Mysql
type MysqlSyncer struct {
	db      *sql.DB
	loader  loader.Loader
	relayer relay.Relayer

	*baseSyncer

	workerCnt    int
	curWorkerIdx int
	workers      []*mysqlWorker
	ctx          context.Context
	cancel       context.CancelFunc
	itemErrCh    chan error
}

type mysqlWorker struct {
	syncer *MysqlSyncer
	ch     chan *Item
	output chan *loader.Txn
}

func (w *mysqlWorker) run() {
	for {
		var txn *loader.Txn
		select {
		case item, ok := <-w.ch:
			if !ok {
				return
			}
			tx, err := translator.TiBinlogToTxn(w.syncer.tableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue, item.ShouldSkip)
			if err != nil {
				select {
				case w.syncer.itemErrCh <- err:
				default:
				}
				w.syncer.cancel()
				return
			}
			tx.Metadata = item
			txn = tx
		case <-w.syncer.ctx.Done():
			return
		}
		select {
		case w.output <- txn:
		case <-w.syncer.ctx.Done():
			return
		}
	}
}

// should only be used for unit test to create mock db
var createDB = loader.CreateDBWithSQLMode

// CreateLoader create the Loader instance.
func CreateLoader(
	db *sql.DB,
	cfg *DBConfig,
	worker int,
	batchSize int,
	queryHistogramVec *prometheus.HistogramVec,
	sqlMode *string,
	destDBType string,
	info *loopbacksync.LoopBackSync,
) (ld loader.Loader, err error) {

	var opts []loader.Option
	opts = append(opts, loader.WorkerCount(worker), loader.BatchSize(batchSize), loader.SaveAppliedTS(destDBType == "tidb"), loader.SetloopBackSyncInfo(info))
	if queryHistogramVec != nil {
		opts = append(opts, loader.Metrics(&loader.MetricsGroup{
			QueryHistogramVec: queryHistogramVec,
			EventCounterVec:   nil,
		}))
	}

	if cfg.SyncMode != 0 {
		mode := loader.SyncMode(cfg.SyncMode)
		opts = append(opts, loader.SyncModeOption(mode))
	}

	ld, err = loader.NewLoader(db, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return
}

// NewMysqlSyncer returns a instance of MysqlSyncer
func NewMysqlSyncer(
	cfg *DBConfig,
	tableInfoGetter translator.TableInfoGetter,
	worker int,
	batchSize int,
	encoderCount int,
	queryHistogramVec *prometheus.HistogramVec,
	sqlMode *string,
	destDBType string,
	relayer relay.Relayer,
	info *loopbacksync.LoopBackSync,
) (*MysqlSyncer, error) {
	if cfg.TLS != nil {
		log.Info("enable TLS to connect downstream MySQL/TiDB")
	}

	db, err := createDB(cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.TLS, sqlMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	syncMode := loader.SyncMode(cfg.SyncMode)
	if syncMode == loader.SyncPartialColumn {
		var oldMode, newMode string
		oldMode, newMode, err = relaxSQLMode(db)
		if err != nil {
			db.Close()
			return nil, errors.Trace(err)
		}

		if newMode != oldMode {
			db.Close()
			db, err = createDB(cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.TLS, &newMode)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	l, err := CreateLoader(db, cfg, worker, batchSize, queryHistogramVec, sqlMode, destDBType, info)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s := &MysqlSyncer{
		db:         db,
		loader:     l,
		relayer:    relayer,
		baseSyncer: newBaseSyncer(tableInfoGetter),
		itemErrCh:  make(chan error, 1),
	}

	if encoderCount <= 0 {
		encoderCount = 1
	}

	for i := 0; i < encoderCount; i++ {
		w := &mysqlWorker{
			syncer: s,
			ch:     make(chan *Item, 2),
			output: make(chan *loader.Txn, 2),
		}
		go w.run()
		s.workers = append(s.workers, w)
	}

	go s.run()
	go s.syncMsg()

	return s, nil
}

// set newMode as the oldMode query from db by removing "STRICT_TRANS_TABLES".
func relaxSQLMode(db *sql.DB) (oldMode string, newMode string, err error) {
	row := db.QueryRow("SELECT @@SESSION.sql_mode;")
	err = row.Scan(&oldMode)
	if err != nil {
		return "", "", errors.Trace(err)
	}

	toRemove := "STRICT_TRANS_TABLES"
	newMode = oldMode

	if !strings.Contains(oldMode, toRemove) {
		return
	}

	// concatenated by "," like: mode1,mode2
	newMode = strings.Replace(newMode, toRemove+",", "", -1)
	newMode = strings.Replace(newMode, ","+toRemove, "", -1)
	newMode = strings.Replace(newMode, toRemove, "", -1)

	return
}

// SetSafeMode make the MysqlSyncer to use safe mode or not
func (m *MysqlSyncer) SetSafeMode(mode bool) bool {
	m.loader.SetSafeMode(mode)
	return true
}

// Sync implements Syncer interface
func (m *MysqlSyncer) Sync(item *Item) error {
	// `relayer` is nil if relay log is disabled.
	if m.relayer != nil {
		pos, err := m.relayer.WriteBinlog(item.Schema, item.Table, item.Binlog, item.PrewriteValue)
		if err != nil {
			return err
		}
		item.RelayLogPos = pos
	}

	select {
	case m.workers[m.curWorkerIdx].ch <- item:
	case e := <-m.itemErrCh:
		return e
	}

	m.curWorkerIdx = (m.curWorkerIdx + 1) % m.workerCnt
	return nil
}

// Close implements Syncer interface
func (m *MysqlSyncer) Close() error {
	m.loader.Close()

	err := <-m.Error()

	if m.relayer != nil {
		closeRelayerErr := m.relayer.Close()
		if err != nil {
			err = closeRelayerErr
		}
	}

	return err
}

func (m *MysqlSyncer) run() {
	var wg sync.WaitGroup

	// handle success
	wg.Add(1)
	go func() {
		defer wg.Done()

		for txn := range m.loader.Successes() {
			item := txn.Metadata.(*Item)
			item.AppliedTS = txn.AppliedTS
			if m.relayer != nil {
				m.relayer.GCBinlog(item.RelayLogPos)
			}
			m.success <- item
		}
		close(m.success)
		log.Info("Successes chan quit")
	}()

	// run loader
	err := m.loader.Run()

	wg.Wait()
	m.db.Close()
	m.setErr(err)
}

func (m *MysqlSyncer) syncMsg() {
	cnt := 0
	for {
		var tx *loader.Txn
		select {
		case txn, ok := <-m.workers[cnt%m.workerCnt].output:
			if !ok {
				return
			}
			tx = txn
		case <-m.ctx.Done():
			return
		}
		select {
		case m.loader.Input() <- tx:
		case <-m.ctx.Done():
			return
		}
		cnt++
	}
}
