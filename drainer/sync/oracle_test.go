// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package sync

import (
	"time"

	"github.com/pingcap/tidb/parser/model"
	router "github.com/pingcap/tidb/util/table-router"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"

	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

var _ = check.Suite(&oracleSuite{})

type oracleSuite struct {
}

type fakeOracleLoader struct {
	loader.Loader
	successes chan *loader.Txn
	input     chan *loader.Txn
}

func (l *fakeOracleLoader) Input() chan<- *loader.Txn {
	return l.input
}

func (l *fakeOracleLoader) Run() error {
	close(l.successes)
	return errors.New("OracleSyncerMockTest")
}

func (l *fakeOracleLoader) Successes() <-chan *loader.Txn {
	return l.successes
}

type fakeSchema struct {
	UpDownSchemaMap map[string]string
}

func (s *fakeSchema) TableByID(id int64) (info *model.TableInfo, ok bool) {
	return nil, false
}
func (s *fakeSchema) SchemaAndTableName(id int64) (string, string, bool) {
	return "", "", false
}
func (s *fakeSchema) CanAppendDefaultValue(id int64, schemaVersion int64) bool {
	return false
}
func (s *fakeSchema) TableBySchemaVersion(id int64, schemaVersion int64) (info *model.TableInfo, ok bool) {
	return nil, false
}

func (s *oracleSuite) TestOracleSyncerAvoidBlock(c *check.C) {
	infoGetter := &fakeSchema{}
	fakeOracleLoaderImpl := &fakeOracleLoader{
		successes: make(chan *loader.Txn),
		input:     make(chan *loader.Txn),
	}
	var rules = []*router.TableRule{
		{SchemaPattern: "test", TablePattern: "*", TargetSchema: "test_routed", TargetTable: "test_table_routed"},
	}
	router, err := router.NewTableRouter(false, rules)
	c.Assert(err, check.IsNil)
	db, _, _ := sqlmock.New()
	syncer := &OracleSyncer{
		db:          db,
		loader:      fakeOracleLoaderImpl,
		baseSyncer:  newBaseSyncer(infoGetter, nil),
		tableRouter: router,
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
		c.Assert(err, check.ErrorMatches, ".*OracleSyncerMockTest.*")
	case <-time.After(time.Second):
		c.Fatal("oracle syncer hasn't quit in 1s after some error occurs in loader")
	}

	finishSync := make(chan struct{})
	go func() {
		err := syncer.Sync(item)
		c.Assert(err, check.ErrorMatches, ".*OracleSyncerMockTest.*")
		close(finishSync)
	}()
	select {
	case <-finishSync:
	case <-time.After(time.Second):
		c.Fatal("oracle syncer hasn't synced item in 1s after some error occurs in loader")
	}
}

type fakeOracleLoaderForRelayer struct {
	loader.Loader
	successes chan *loader.Txn
	input     chan *loader.Txn
}

func (l *fakeOracleLoaderForRelayer) Input() chan<- *loader.Txn {
	return l.input
}

func (l *fakeOracleLoaderForRelayer) Run() error {
	go func() {
		for txn := range l.input {
			l.successes <- txn
		}
	}()
	return nil
}

func (l *fakeOracleLoaderForRelayer) Successes() <-chan *loader.Txn {
	return l.successes
}

func (l *fakeOracleLoaderForRelayer) Close() {
	close(l.successes)
}

func (s *oracleSuite) TestOracleSyncerWithRelayer(c *check.C) {
	infoGetter := &fakeSchema{}
	fakeOracleLoaderImpl := &fakeOracleLoaderForRelayer{
		successes: make(chan *loader.Txn, 8),
		input:     make(chan *loader.Txn),
	}
	var rules = []*router.TableRule{
		{SchemaPattern: "test", TablePattern: "*", TargetSchema: "test_routed", TargetTable: "test_table_routed"},
	}
	router, err := router.NewTableRouter(false, rules)
	c.Assert(err, check.IsNil)
	db, _, _ := sqlmock.New()

	dir := c.MkDir()
	relayer, err := relay.NewRelayer(dir, 10, nil)
	c.Assert(relayer, check.NotNil)
	c.Assert(err, check.IsNil)
	syncer := &OracleSyncer{
		db:          db,
		loader:      fakeOracleLoaderImpl,
		relayer:     relayer,
		baseSyncer:  newBaseSyncer(infoGetter, nil),
		tableRouter: router,
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
			c.Fatal("oracle syncer hasn't synced item in 1s after some error occurs in loader")
		}
	}

	names, err := binlogfile.ReadBinlogNames(dir)
	c.Assert(err, check.IsNil)
	// There would be 2 files: the last written file, the new created empty file.
	// The previous files should be removed.
	c.Assert(len(names), check.Equals, 2)
}
