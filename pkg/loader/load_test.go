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
	"database/sql"
	"reflect"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	check "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type LoadSuite struct {
}

var _ = check.Suite(&LoadSuite{})

func (cs *LoadSuite) SetUpTest(c *check.C) {
}

func (cs *LoadSuite) TearDownTest(c *check.C) {
}

func (cs *LoadSuite) TestOptions(c *check.C) {
	var o options
	WorkerCount(42)(&o)
	BatchSize(1024)(&o)
	SaveAppliedTS(true)(&o)
	var mg MetricsGroup
	Metrics(&mg)(&o)
	c.Assert(o.workerCount, check.Equals, 42)
	c.Assert(o.batchSize, check.Equals, 1024)
	c.Assert(o.metrics, check.Equals, &mg)
	c.Assert(o.saveAppliedTS, check.Equals, true)
}

func (cs *LoadSuite) TestGetExecutor(c *check.C) {
	db, _, err := sqlmock.New()
	c.Assert(err, check.IsNil)
	loader := &loaderImpl{
		db:        db,
		batchSize: 1234,
		metrics: &MetricsGroup{
			QueryHistogramVec: &prometheus.HistogramVec{},
		},
		ctx: context.Background(),
	}
	var e *executor = loader.getExecutor()
	c.Assert(e.db, check.DeepEquals, loader.db)
	c.Assert(e.batchSize, check.Equals, loader.batchSize)
	c.Assert(e.queryHistogramVec, check.NotNil)
}

func (cs *LoadSuite) TestCountEvents(c *check.C) {
	dmls := []*DML{
		{Tp: UpdateDMLType},
		{Tp: InsertDMLType},
		{Tp: DeleteDMLType},
		{Tp: UpdateDMLType},
		{Tp: InsertDMLType},
	}
	nInsert, nDelete, nUpdate := countEvents(dmls)
	c.Assert(nInsert, check.Equals, float64(2))
	c.Assert(nDelete, check.Equals, float64(1))
	c.Assert(nUpdate, check.Equals, float64(2))
}

func (cs *LoadSuite) TestSafeMode(c *check.C) {
	loader := &loaderImpl{}
	c.Assert(loader.GetSafeMode(), check.IsFalse)
	loader.SetSafeMode(true)
	c.Assert(loader.GetSafeMode(), check.IsTrue)
	loader.SetSafeMode(false)
	c.Assert(loader.GetSafeMode(), check.IsFalse)
}

func (cs *LoadSuite) TestSuccesses(c *check.C) {
	loader := &loaderImpl{successTxn: make(chan *Txn, 64)}
	txns := []*Txn{
		{Metadata: 1},
		{Metadata: 3},
		{Metadata: 5},
	}
	loader.markSuccess(txns...)

	successes := loader.Successes()
	c.Assert(successes, check.HasLen, len(txns))
	for _, txn := range txns {
		get := <-successes
		c.Assert(get, check.DeepEquals, txn)
	}
}

func (cs *LoadSuite) TestNewBatchManager(c *check.C) {
	loader := &loaderImpl{batchSize: 1000, workerCount: 10}
	bm := newBatchManager(loader)
	c.Assert(bm.limit, check.Equals, 30000)

	c.Assert(reflect.ValueOf(bm.fExecDMLs).Pointer(), check.Equals, reflect.ValueOf(loader.execDMLs).Pointer())
	c.Assert(reflect.ValueOf(bm.fExecDDL).Pointer(), check.Equals, reflect.ValueOf(loader.execDDL).Pointer())
}

func (cs *LoadSuite) TestNewClose(c *check.C) {
	db, _, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	loader, err := NewLoader(db)
	c.Assert(err, check.IsNil)

	loader.Close()
}

func (cs *LoadSuite) TestSetDMLInfo(c *check.C) {
	info := tableInfo{columns: []string{"id", "name"}}
	origGet := utilGetTableInfo
	utilGetTableInfo = func(db *sql.DB, schema string, table string) (*tableInfo, error) {
		return &info, nil
	}
	defer func() {
		utilGetTableInfo = origGet
	}()
	ld := loaderImpl{}

	dml := DML{Database: "test", Table: "t1"}
	c.Assert(dml.info, check.IsNil)
	err := ld.setDMLInfo(&dml)
	c.Assert(err, check.IsNil)
	c.Assert(dml.info, check.Equals, &info)
}

func (cs *LoadSuite) TestFilterGeneratedCols(c *check.C) {
	dml := DML{
		Values: map[string]interface{}{
			"first": "Jim",
			"last":  "Green",
			"full":  "Jim Green",
		},
		info: &tableInfo{
			columns: []string{"first", "last"},
		},
	}
	filterGeneratedCols(&dml)
	c.Assert(dml.Values, check.DeepEquals, map[string]interface{}{
		"first": "Jim",
		"last":  "Green",
	})
}

type groupDMLsSuite struct{}

var _ = check.Suite(&groupDMLsSuite{})

func (s *groupDMLsSuite) TestSingleDMLsOnlyIfDisableMerge(c *check.C) {
	ld := loaderImpl{merge: false}
	dmls := []*DML{
		{Tp: UpdateDMLType},
		{Tp: UpdateDMLType},
		{Tp: InsertDMLType},
	}
	batch, single := ld.groupDMLs(dmls)
	c.Assert(batch, check.HasLen, 0)
	c.Assert(single, check.HasLen, 3)
}

func (s *groupDMLsSuite) TestGroupByTableName(c *check.C) {
	ld := loaderImpl{merge: true}
	canBatch := tableInfo{primaryKey: &indexInfo{}}
	onlySingle := tableInfo{}
	dmls := []*DML{
		{Table: "test1", info: &canBatch},
		{Table: "test1", info: &canBatch},
		{Table: "test2", info: &onlySingle},
		{Table: "test1", info: &canBatch},
		{Table: "test2", info: &onlySingle},
	}
	batch, single := ld.groupDMLs(dmls)
	c.Assert(batch, check.HasLen, 1)
	c.Assert(batch[dmls[0].TableName()], check.HasLen, 3)
	c.Assert(single, check.HasLen, 2)
}

type getTblInfoSuite struct{}

var _ = check.Suite(&getTblInfoSuite{})

func (s *getTblInfoSuite) TestShouldCacheResult(c *check.C) {
	origGet := utilGetTableInfo
	nCalled := 0
	utilGetTableInfo = func(db *sql.DB, schema string, table string) (info *tableInfo, err error) {
		nCalled++
		return &tableInfo{columns: []string{"id", "name"}}, nil
	}
	defer func() {
		utilGetTableInfo = origGet
	}()
	ld := loaderImpl{}

	info, err := ld.getTableInfo("test", "contacts")
	c.Assert(err, check.IsNil)
	c.Assert(info.columns[1], check.Equals, "name")

	info, err = ld.getTableInfo("test", "contacts")
	c.Assert(err, check.IsNil)
	c.Assert(info.columns[1], check.Equals, "name")

	c.Assert(nCalled, check.Equals, 1)
}

type isCreateDBDDLSuite struct{}

var _ = check.Suite(&isCreateDBDDLSuite{})

func (s *isCreateDBDDLSuite) TestInvalidSQL(c *check.C) {
	c.Assert(isCreateDatabaseDDL("INSERT INTO Y a b c;"), check.IsFalse)
}

func (s *isCreateDBDDLSuite) TestNonCreateDBSQL(c *check.C) {
	c.Assert(isCreateDatabaseDDL("SELECT 1;"), check.IsFalse)
	c.Assert(isCreateDatabaseDDL(`INSERT INTO tbl(id, name) VALUES(1, "test";`), check.IsFalse)
}

func (s *isCreateDBDDLSuite) TestCreateDBSQL(c *check.C) {
	c.Assert(isCreateDatabaseDDL("CREATE DATABASE test;"), check.IsTrue)
	c.Assert(isCreateDatabaseDDL("create database `db2`;"), check.IsTrue)
}

type needRefreshTableInfoSuite struct{}

var _ = check.Suite(&needRefreshTableInfoSuite{})

func (s *needRefreshTableInfoSuite) TestNeedRefreshTableInfo(c *check.C) {
	cases := map[string]bool{
		"DROP TABLE a":           false,
		"DROP DATABASE a":        false,
		"TRUNCATE TABLE a":       false,
		"CREATE DATABASE a":      false,
		"CREATE TABLE a(id int)": true,
	}

	for sql, res := range cases {
		c.Assert(needRefreshTableInfo(sql), check.Equals, res)
	}
}

type execDDLSuite struct{}

var _ = check.Suite(&execDDLSuite{})

func (s *execDDLSuite) TestShouldExecInTransaction(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	loader := &loaderImpl{db: db, ctx: context.Background()}

	ddl := DDL{SQL: "CREATE TABLE"}
	err = loader.execDDL(&ddl)
	c.Assert(err, check.IsNil)
}

func (s *execDDLSuite) TestShouldUseDatabase(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	mock.ExpectBegin()
	mock.ExpectExec("use `test_db`").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	loader := &loaderImpl{db: db, ctx: context.Background()}

	ddl := DDL{SQL: "CREATE TABLE", Database: "test_db"}
	err = loader.execDDL(&ddl)
	c.Assert(err, check.IsNil)
}

type batchManagerSuite struct{}

var _ = check.Suite(&batchManagerSuite{})

func (s *batchManagerSuite) TestShouldExecDDLImmediately(c *check.C) {
	var executed *DDL
	var cbTxn *Txn
	bm := batchManager{
		limit: 1024,
		fExecDDL: func(ddl *DDL) error {
			executed = ddl
			return nil
		},
		fDDLSuccessCallback: func(t *Txn) {
			cbTxn = t
		},
	}
	txn := Txn{
		DDL: &DDL{Database: "test", Table: "Hey", SQL: "CREATE"},
	}
	err := bm.put(&txn)
	c.Assert(err, check.IsNil)
	c.Assert(executed, check.DeepEquals, txn.DDL)
	c.Assert(*cbTxn, check.DeepEquals, txn)
}

func (s *batchManagerSuite) TestShouldHandleDDLError(c *check.C) {
	var nCalled int
	bm := batchManager{
		limit: 1024,
		fDDLSuccessCallback: func(t *Txn) {
			nCalled++
		},
	}
	bm.fExecDDL = func(ddl *DDL) error {
		return errors.New("DDL")
	}
	txn := Txn{
		DDL: &DDL{Database: "test", Table: "Hey", SQL: "CREATE"},
	}
	err := bm.put(&txn)
	c.Assert(err, check.ErrorMatches, "DDL")
	c.Assert(nCalled, check.Equals, 0)

	bm.fExecDDL = func(ddl *DDL) error {
		return &mysql.MySQLError{Number: 1146}
	}

	err = bm.put(&txn)
	c.Assert(err, check.IsNil)
	c.Assert(nCalled, check.Equals, 1)
}

func (s *batchManagerSuite) TestShouldExecAccumulatedDMLs(c *check.C) {
	var executed []*DML
	var calledback []*Txn
	bm := batchManager{
		limit: 3,
		fExecDMLs: func(dmls []*DML) error {
			executed = append(executed, dmls...)
			return nil
		},
		fDMLsSuccessCallback: func(txns ...*Txn) {
			calledback = append(calledback, txns...)
		},
	}
	var txns []*Txn
	// Set up the number of DMLs so that only the first 3 txns get executed
	nDMLs := []int{1, 1, 3, 2}
	for _, n := range nDMLs {
		var dmls []*DML
		for i := 0; i < n; i++ {
			dmls = append(dmls, &DML{})
		}
		txn := Txn{DMLs: dmls}
		txns = append(txns, &txn)
		err := bm.put(&txn)
		c.Assert(err, check.IsNil)
	}
	c.Assert(executed, check.HasLen, 5)
	c.Assert(calledback, check.DeepEquals, txns[:3])
	c.Assert(bm.dmls, check.HasLen, 2)
	c.Assert(bm.txns, check.HasLen, 1)
}

type runSuite struct{}

var _ = check.Suite(&runSuite{})

func (s *runSuite) TestShouldExecuteAllPendingDMLsOnClose(c *check.C) {
	var executed []*DML
	var cbTxns []*Txn
	origF := fNewBatchManager
	fNewBatchManager = func(s *loaderImpl) *batchManager {
		return &batchManager{
			limit: 1024,
			fExecDMLs: func(dmls []*DML) error {
				executed = dmls
				return nil
			},
			fDMLsSuccessCallback: func(txns ...*Txn) {
				cbTxns = txns
			},
		}
	}
	defer func() { fNewBatchManager = origF }()

	loader := &loaderImpl{
		input:      make(chan *Txn, 10),
		successTxn: make(chan *Txn, 10),
	}
	go func() {
		for i := 0; i < 7; i++ {
			dmls := []*DML{
				{Tp: UpdateDMLType},
				{Tp: InsertDMLType},
			}
			loader.input <- &Txn{DMLs: dmls}
		}
		close(loader.input)
	}()

	signal := make(chan struct{})
	go func() {
		err := loader.Run()
		c.Assert(err, check.IsNil)
		close(signal)
	}()

	select {
	case <-signal:
	case <-time.After(time.Second):
		c.Fatal("Run doesn't stop in time after input's closed")
	}

	c.Assert(executed, check.HasLen, 14)
	c.Assert(cbTxns, check.HasLen, 7)
}

func (s *runSuite) TestShouldFlushWhenInputIsEmpty(c *check.C) {
	var executed []*DML
	origF := fNewBatchManager
	fNewBatchManager = func(s *loaderImpl) *batchManager {
		return &batchManager{
			limit: 1024,
			fExecDMLs: func(dmls []*DML) error {
				executed = dmls
				return nil
			},
			fDMLsSuccessCallback: func(txns ...*Txn) {},
		}
	}
	defer func() { fNewBatchManager = origF }()

	loader := &loaderImpl{
		input:      make(chan *Txn, 10),
		successTxn: make(chan *Txn, 10),
	}

	go func() {
		err := loader.Run()
		c.Assert(err, check.IsNil)
	}()
	defer close(loader.input)

	addTxn := func(i int) {
		dmls := []*DML{
			{Tp: UpdateDMLType},
		}
		loader.input <- &Txn{DMLs: dmls}
	}

	for i := 0; i < 3; i++ {
		addTxn(i)
	}
	// Wait 10ms for loader to exhaust the input channel
	time.Sleep(10 * time.Millisecond)
	c.Assert(executed, check.HasLen, 3)
	for i := 0; i < 7; i++ {
		addTxn(i)
	}
	time.Sleep(10 * time.Millisecond)
	c.Assert(executed, check.HasLen, 7)
}

type markSuccessesSuite struct{}

var _ = check.Suite(&markSuccessesSuite{})

func (ms *markSuccessesSuite) TestShouldSetAppliedTS(c *check.C) {
	origF := fGetAppliedTS
	defer func() {
		fGetAppliedTS = origF
	}()
	fGetAppliedTS = func(*sql.DB) int64 {
		return 88881234
	}
	loader := &loaderImpl{saveAppliedTS: true, successTxn: make(chan *Txn, 64)}
	loader.markSuccess([]*Txn{}...) // Make sure it won't break when no txns are passed
	txns := []*Txn{
		{Metadata: 1},
		{Metadata: 3},
		{Metadata: 5},
	}
	loader.markSuccess(txns...)
	c.Assert(txns[len(txns)-1].AppliedTS, check.Equals, fGetAppliedTS(nil))

	txns = []*Txn{
		{Metadata: 7},
		{Metadata: 8},
	}
	loader.markSuccess(txns...)
	c.Assert(txns[len(txns)-1].AppliedTS, check.Equals, int64(0))

	originUpdateLastAppliedTSInterval := updateLastAppliedTSInterval
	updateLastAppliedTSInterval = 0
	defer func() {
		updateLastAppliedTSInterval = originUpdateLastAppliedTSInterval
	}()
	txns = []*Txn{
		{Metadata: 9},
		{Metadata: 10},
	}
	loader.markSuccess(txns...)
	c.Assert(txns[len(txns)-1].AppliedTS, check.Equals, int64(88881234))
}
