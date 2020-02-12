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
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

var _ = check.Suite(&mysqlSuite{})

type mysqlSuite struct {
}

type fakeMySQLLoader struct {
	loader.Loader
	successes chan *loader.Txn
	input     chan *loader.Txn
}

func (l *fakeMySQLLoader) Input() chan<- *loader.Txn {
	return l.input
}

func (l *fakeMySQLLoader) Run() error {
	close(l.successes)
	return errors.New("MySQLSyncerMockTest")
}

func (l *fakeMySQLLoader) Successes() <-chan *loader.Txn {
	return l.successes
}

func (s *mysqlSuite) TestMySQLSyncerAvoidBlock(c *check.C) {
	var infoGetter translator.TableInfoGetter
	// create mysql syncer
	fakeMySQLLoaderImpl := &fakeMySQLLoader{
		successes: make(chan *loader.Txn),
		input:     make(chan *loader.Txn),
	}
	db, _, _ := sqlmock.New()
	syncer := &MysqlSyncer{
		db:         db,
		loader:     fakeMySQLLoaderImpl,
		baseSyncer: newBaseSyncer(infoGetter),
	}
	go syncer.run()
	gen := translator.BinlogGenerator{}
	gen.SetDDL()
	item := &Item{
		Binlog:        gen.TiBinlog,
		PrewriteValue: gen.PV,
		Schema:        gen.Schema,
		Table:         gen.Table,
	}
	select {
	case err := <-syncer.Error():
		c.Assert(err, check.ErrorMatches, ".*MySQLSyncerMockTest.*")
	case <-time.After(time.Second):
		c.Fatal("mysql syncer hasn't quit in 1s after some error occurs in loader")
	}

	finishSync := make(chan struct{})
	go func() {
		err := syncer.Sync(item)
		c.Assert(err, check.ErrorMatches, ".*MySQLSyncerMockTest.*")
		close(finishSync)
	}()
	select {
	case <-finishSync:
	case <-time.After(time.Second):
		c.Fatal("mysql syncer hasn't synced item in 1s after some error occurs in loader")
	}
}

func (s *mysqlSuite) TestRelaxSQLMode(c *check.C) {
	tests := []struct {
		oldMode string
		newMode string
	}{
		{"ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE", "ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE"},
		{"ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,STRICT_TRANS_TABLES", "ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE"},
		{"STRICT_TRANS_TABLES", ""},
		{"ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE", "ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE"},
	}

	for _, test := range tests {
		db, dbMock, err := sqlmock.New()
		c.Assert(err, check.IsNil)

		rows := sqlmock.NewRows([]string{"@@SESSION.sql_mode"}).
			AddRow(test.oldMode)
		dbMock.ExpectQuery("SELECT @@SESSION.sql_mode;").WillReturnRows(rows)

		getOld, getNew, err := relaxSQLMode(db)
		c.Assert(err, check.IsNil)
		c.Assert(getOld, check.Equals, test.oldMode)
		c.Assert(getNew, check.Equals, test.newMode)
	}
}

type fakeMySQLLoaderForRelayer struct {
	loader.Loader
	successes chan *loader.Txn
	input     chan *loader.Txn
}

func (l *fakeMySQLLoaderForRelayer) Input() chan<- *loader.Txn {
	return l.input
}

func (l *fakeMySQLLoaderForRelayer) Run() error {
	go func() {
		for txn := range l.input {
			l.successes <- txn
		}
	}()
	return nil
}

func (l *fakeMySQLLoaderForRelayer) Successes() <-chan *loader.Txn {
	return l.successes
}

func (l *fakeMySQLLoaderForRelayer) Close() {
	close(l.successes)
}

func (s *mysqlSuite) TestMySQLSyncerWithRelayer(c *check.C) {
	var infoGetter translator.TableInfoGetter
	// create mysql syncer
	fakeMySQLLoaderImpl := &fakeMySQLLoaderForRelayer{
		successes: make(chan *loader.Txn, 8),
		input:     make(chan *loader.Txn),
	}
	db, _, _ := sqlmock.New()

	dir := c.MkDir()
	relayer, err := relay.NewRelayer(dir, 10, nil)
	c.Assert(relayer, check.NotNil)
	c.Assert(err, check.IsNil)
	syncer := &MysqlSyncer{
		db:         db,
		loader:     fakeMySQLLoaderImpl,
		relayer:    relayer,
		baseSyncer: newBaseSyncer(infoGetter),
	}
	defer syncer.Close()

	go syncer.run()
	gen := translator.BinlogGenerator{}
	gen.SetDDL()

	for i := 0; i < 5; i++ {
		item := &Item{
			Binlog:        gen.TiBinlog,
			PrewriteValue: gen.PV,
			Schema:        gen.Schema,
			Table:         gen.Table,
		}
		err = syncer.Sync(item)
		c.Assert(err, check.IsNil)
	}

	// wait for all binlogs processed
	for i := 0; i < 5; i++ {
		select {
		case <-syncer.Successes():
		case <-time.After(time.Second):
			c.Fatal("mysql syncer hasn't synced item in 1s after some error occurs in loader")
		}
	}

	names, err := binlogfile.ReadBinlogNames(dir)
	c.Assert(err, check.IsNil)
	// There would be 2 files: the last written file, the new created empty file.
	// The previous files should be removed.
	c.Assert(len(names), check.Equals, 2)
}
