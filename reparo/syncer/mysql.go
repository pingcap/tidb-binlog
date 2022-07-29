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

package syncer

// execute sql to mysql/tidb

import (
	"database/sql"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

// DBConfig is the DB configuration.
type DBConfig struct {
	Host     string `toml:"host" json:"host"`
	User     string `toml:"user" json:"user"`
	Password string `toml:"password" json:"password"`
	Port     int    `toml:"port" json:"port"`
}

type mysqlSyncer struct {
	db *sql.DB

	loader loader.Loader

	loaderQuit chan struct{}
	loaderErr  error
}

var (
	_ Syncer = &mysqlSyncer{}
)

// should be only used for unit test to create mock db
var createDB = loader.CreateDB

func newMysqlSyncer(cfg *DBConfig, worker int, batchSize int, safemode bool, metricsGroup *loader.MetricsGroup) (*mysqlSyncer, error) {
	db, err := createDB(cfg.User, cfg.Password, cfg.Host, cfg.Port, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return newMysqlSyncerFromSQLDB(db, worker, batchSize, safemode, metricsGroup)
}

func newMysqlSyncerFromSQLDB(db *sql.DB, worker int, batchSize int, safemode bool, metricsGroup *loader.MetricsGroup) (*mysqlSyncer, error) {
	opts := []loader.Option{loader.WorkerCount(worker), loader.BatchSize(batchSize)}
	if metricsGroup != nil {
		opts = append(opts, loader.Metrics(metricsGroup))
	}
	loader, err := loader.NewLoader(db, opts...)
	if err != nil {
		return nil, errors.Annotate(err, "new loader failed")
	}

	loader.SetSafeMode(safemode)
	syncer := &mysqlSyncer{db: db, loader: loader}
	syncer.runLoader()

	return syncer, nil
}

type item struct {
	binlog *pb.Binlog
	cb     func(binlog *pb.Binlog)
}

func (m *mysqlSyncer) Sync(pbBinlog *pb.Binlog, cb func(binlog *pb.Binlog)) error {
	txn, err := pbBinlogToTxn(pbBinlog)
	if err != nil {
		return errors.Annotate(err, "pbBinlogToTxn failed")
	}

	item := &item{binlog: pbBinlog, cb: cb}
	txn.Metadata = item

	select {
	case <-m.loaderQuit:
		return m.loaderErr
	case m.loader.Input() <- txn:
		return nil
	}
}

func (m *mysqlSyncer) Close() error {
	m.loader.Close()

	<-m.loaderQuit

	m.db.Close()

	return m.loaderErr
}

func (m *mysqlSyncer) runLoader() {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for txn := range m.loader.Successes() {
			item := txn.Metadata.(*item)
			item.cb(item.binlog)
		}
		log.Info("Successes chan quit")
		wg.Done()
	}()

	m.loaderQuit = make(chan struct{})
	m.loaderErr = nil
	go func() {
		err := m.loader.Run()
		if err != nil {
			m.loaderErr = err
		}
		wg.Wait()
		close(m.loaderQuit)
	}()
}
