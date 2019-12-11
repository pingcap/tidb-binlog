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
	"database/sql"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	"github.com/prometheus/client_golang/prometheus"
)

var _ Syncer = &MysqlSyncer{}

// MysqlSyncer sync binlog to Mysql
type MysqlSyncer struct {
	db     *sql.DB
	loader loader.Loader
	relayer relay.Relayer

	*baseSyncer
}

// should only be used for unit test to create mock db
var createDB = loader.CreateDBWithSQLMode

// NewMysqlSyncer returns a instance of MysqlSyncer
func NewMysqlSyncer(cfg *DBConfig, tableInfoGetter translator.TableInfoGetter, worker int, batchSize int, queryHistogramVec *prometheus.HistogramVec, sqlMode *string, destDBType string, relayLogDir string, relayLogSize int64) (*MysqlSyncer, error) {
	db, err := createDB(cfg.User, cfg.Password, cfg.Host, cfg.Port, sqlMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var opts []loader.Option
	opts = append(opts, loader.WorkerCount(worker), loader.BatchSize(batchSize), loader.SaveAppliedTS(destDBType == "tidb"))
	if queryHistogramVec != nil {
		opts = append(opts, loader.Metrics(&loader.MetricsGroup{
			QueryHistogramVec: queryHistogramVec,
			EventCounterVec:   nil,
		}))
	}

	loader, err := loader.NewLoader(db, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	relayer, err := relay.NewRelayer(relayLogDir, relayLogSize, tableInfoGetter)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s := &MysqlSyncer{
		db:         db,
		loader:     loader,
		relayer:    relayer,
		baseSyncer: newBaseSyncer(tableInfoGetter),
	}

	go s.run()

	return s, nil
}

// SetSafeMode make the MysqlSyncer to use safe mode or not
func (m *MysqlSyncer) SetSafeMode(mode bool) {
	m.loader.SetSafeMode(mode)
}

// Sync implements Syncer interface
func (m *MysqlSyncer) Sync(item *Item) error {
	if err := m.relayer.WriteBinlog(item); err != nil {
		return err
	}

	txn, err := translator.TiBinlogToTxn(m.tableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue)
	if err != nil {
		return errors.Trace(err)
	}

	txn.Metadata = item

	select {
	case <-m.errCh:
		return m.err
	case m.loader.Input() <- txn:
		return nil
	}
}

// Close implements Syncer interface
func (m *MysqlSyncer) Close() error {
	m.loader.Close()

	err := <-m.Error()

	closeRelayerErr := m.relayer.Close()
	if err != nil {
		err = closeRelayerErr
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
			m.relayer.GCBinlog(item)
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
