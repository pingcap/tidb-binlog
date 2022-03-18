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
	"sync"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
)

type LoadSuite struct {
}

var _ = check.Suite(&LoadSuite{})

func (cs *LoadSuite) SetUpTest(c *check.C) {
}

func (cs *LoadSuite) TearDownTest(c *check.C) {
}

func (cs *LoadSuite) TestTiFlash(c *check.C) {
	sql := "ALTER TABLE t SET TIFLASH REPLICA 3 LOCATION LABELS \"rack\", \"host\", \"abc\""
	res := isSetTiFlashReplica(sql)
	c.Assert(res, check.IsTrue)

	sql = "create table a(id int)"
	res = isSetTiFlashReplica(sql)
	c.Assert(res, check.IsFalse)
}

func (cs *LoadSuite) TestRemoveOrphanCols(c *check.C) {
	dml := &DML{
		Values: map[string]interface{}{
			"exist1":  11,
			"exist2":  22,
			"orhpan1": 11,
			"orhpan2": 22,
		},
		OldValues: map[string]interface{}{
			"exist1":  1,
			"exist2":  2,
			"orhpan1": 1,
			"orhpan2": 2,
		},
	}

	info := &tableInfo{
		columns: []string{"exist1", "exist2"},
	}

	removeOrphanCols(info, dml)
	c.Assert(dml.Values, check.DeepEquals, map[string]interface{}{
		"exist1": 11,
		"exist2": 22,
	})
	c.Assert(dml.OldValues, check.DeepEquals, map[string]interface{}{
		"exist1": 1,
		"exist2": 2,
	})
}

func (cs *LoadSuite) TestDisableDispatch(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	utilGetTableInfo := func(db *sql.DB, schema string, table string) (*tableInfo, error) {
		return &tableInfo{columns: []string{"id"}}, nil
	}

	ldi, err := NewLoader(db, EnableDispatch(false))
	ld := ldi.(*loaderImpl)
	c.Assert(err, check.IsNil)
	ld.getTableInfoFromDB = utilGetTableInfo

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for range ld.Successes() {
			c.Log("get success")
		}
		wg.Done()
		log.Info("succ quit")
	}()

	var runErr error
	go func() {
		runErr = ld.Run()
		c.Assert(err, check.IsNil)
		log.Info("run quit")
		wg.Done()
	}()

	dml := DML{
		Database: "test",
		Table:    "test",
		Tp:       InsertDMLType,
		Values: map[string]interface{}{
			"id": 1,
		},
	}

	txn := new(Txn)
	for i := 0; i < 100; i++ {
		txn.DMLs = append(txn.DMLs, &dml)
	}

	// the txn must be execute in one txn at downstream.
	mock.ExpectBegin()
	for i := 0; i < 100; i++ {
		mock.ExpectExec("INSERT INTO .*").WithArgs(1).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectCommit()
	ld.Input() <- txn
	ld.Close()

	wg.Wait()
	c.Assert(runErr, check.IsNil)

	err = mock.ExpectationsWereMet()
	c.Assert(err, check.IsNil)
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

	loader, err := NewLoader(db, SyncModeOption(SyncPartialColumn))
	c.Assert(err, check.IsNil)
	c.Assert(loader.(*loaderImpl).syncMode, check.Equals, SyncPartialColumn)

	loader.Close()
}

func (cs *LoadSuite) TestSetDMLInfo(c *check.C) {
	info := tableInfo{columns: []string{"id", "name"}}
	utilGetTableInfo := func(db *sql.DB, schema string, table string) (*tableInfo, error) {
		return &info, nil
	}
	ld := loaderImpl{getTableInfoFromDB: utilGetTableInfo}

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
	canBatch := tableInfo{primaryKey: &indexInfo{}, uniqueKeys: []indexInfo{{}}}
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
	nCalled := 0
	utilGetTableInfo := func(db *sql.DB, schema string, table string) (info *tableInfo, err error) {
		nCalled++
		return &tableInfo{columns: []string{"id", "name"}}, nil
	}
	ld := loaderImpl{getTableInfoFromDB: utilGetTableInfo}

	info, err := ld.getTableInfo("test", "contacts")
	c.Assert(err, check.IsNil)
	c.Assert(info.columns[1], check.Equals, "name")

	info, err = ld.getTableInfo("test", "contacts")
	c.Assert(err, check.IsNil)
	c.Assert(info.columns[1], check.Equals, "name")

	c.Assert(nCalled, check.Equals, 1)
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
	mock.ExpectExec("CREATE TABLE `t` \\(`id` INT\\)").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	loader := &loaderImpl{db: db, ctx: context.Background(), destDBType: "mysql"}

	ddl := DDL{SQL: "CREATE TABLE `t` (`id` INT)"}
	err = loader.execDDL(&ddl)
	c.Assert(err, check.IsNil)
}

func (s *execDDLSuite) TestOracleTruncateDDL(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	mock.ExpectBegin()
	mock.ExpectExec("BEGIN test.do_truncate\\('test.t1',''\\);END;").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	loader := &loaderImpl{db: db, ctx: context.Background(), destDBType: "oracle"}

	ddl := DDL{SQL: "truncate table t1", Database: "test", Table: "t1"}
	err = loader.execDDL(&ddl)
	c.Assert(err, check.IsNil)
}

func (s *execDDLSuite) TestIsTruncateTableStmt(c *check.C) {
	sql := "truncate table test.t1"
	c.Assert(isTruncateTableStmt(sql), check.IsTrue)

	sql = "alter table test.t1 add column c3 int"
	c.Assert(isTruncateTableStmt(sql), check.IsFalse)
}

func (s *execDDLSuite) TestShouldUseDatabase(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	mock.ExpectBegin()
	mock.ExpectExec("use `test_db`").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE `t` \\(`id` INT\\)").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	loader := &loaderImpl{db: db, ctx: context.Background(), destDBType: "mysql"}

	ddl := DDL{SQL: "CREATE TABLE `t` (`id` INT)", Database: "test_db"}
	err = loader.execDDL(&ddl)
	c.Assert(err, check.IsNil)
}

type batchManagerSuite struct{}

var _ = check.Suite(&batchManagerSuite{})

func (s *batchManagerSuite) TestShouldExecDDLImmediately(c *check.C) {
	var executed *DDL
	var cbTxn *Txn
	bm := batchManager{
		limit:          1024,
		enableDispatch: true,
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
		limit:          1024,
		enableDispatch: true,
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
		limit:          3,
		enableDispatch: true,
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

type txnManagerSuite struct{}

var _ = check.Suite(&txnManagerSuite{})

func inputTxnInTime(c *check.C, input chan *Txn, txn *Txn, timeLimit time.Duration) {
	select {
	case input <- txn:
	case <-time.After(timeLimit):
		c.Fatal("txnManager gets blocked while receiving txns")
	}
}

func outputTxnInTime(c *check.C, output chan *Txn, timeLimit time.Duration) *Txn {
	if timeLimit != 0 {
		select {
		case t := <-output:
			return t
		case <-time.After(timeLimit):
			c.Fatal("Fail to pick txn from txnManager")
		}
	} else {
		select {
		case t := <-output:
			return t
		default:
			c.Fatal("Fail to pick txn from txnManager")
		}
	}
	return nil
}

func (s *txnManagerSuite) TestRunTxnManager(c *check.C) {
	input := make(chan *Txn)
	dmls := []*DML{
		{Tp: UpdateDMLType},
		{Tp: InsertDMLType},
		{Tp: UpdateDMLType},
	}
	txn := &Txn{DMLs: dmls}
	txnManager := newTxnManager(14, input)
	output := txnManager.run()
	// send 5 txns (size 3) to txnManager, the 5th txn should get blocked at cond.Wait()
	for i := 0; i < 5; i++ {
		inputTxnInTime(c, input, txn, time.Millisecond)
	}
	c.Assert(output, check.HasLen, 4)
	// Next txn should be blocked
	select {
	case input <- txn:
		c.Fatal("txnManager doesn't block the txn when room is not enough")
	default:
	}
	c.Assert(output, check.HasLen, 4)
	// pick one txn from output channel
	t := outputTxnInTime(c, output, 0)
	txnManager.pop(t)
	c.Assert(t, check.DeepEquals, txn)
	// Now txn won't be blocked but txnManager should be blocked at cond.Wait()
	inputTxnInTime(c, input, txn, 10*time.Microsecond)
	// close txnManager and output should be closed when txnManager is closed
	txnManager.Close()
	outputClose := make(chan struct{})
	go func() {
		for t := range output {
			c.Assert(t, check.DeepEquals, txn)
		}
		close(outputClose)
	}()
	select {
	case <-outputClose:
	case <-time.After(time.Second):
		c.Fatal("txnManager fails to end run()... and close output channel in 1s, may get blocked")
	}
}

func (s *txnManagerSuite) TestAddBigTxn(c *check.C) {
	input := make(chan *Txn)
	txnSmall := &Txn{DMLs: []*DML{{Tp: UpdateDMLType}}}
	txnBig := &Txn{DMLs: []*DML{
		{Tp: UpdateDMLType},
		{Tp: InsertDMLType},
		{Tp: UpdateDMLType},
	}}
	txnManager := newTxnManager(1, input)
	output := txnManager.run()
	inputTxnInTime(c, input, txnSmall, 50*time.Microsecond)
	inputTxnInTime(c, input, txnBig, 10*time.Microsecond)

	t := outputTxnInTime(c, output, 0)
	txnManager.pop(t)
	c.Assert(t, check.DeepEquals, txnSmall)

	t = outputTxnInTime(c, output, 10*time.Microsecond)
	txnManager.pop(t)
	c.Assert(t, check.DeepEquals, txnBig)

	txnManager.Close()
	select {
	case _, ok := <-output:
		c.Assert(ok, check.Equals, false)
	case <-time.After(time.Second):
		c.Fatal("txnManager fails to end run()... and close output channel in 1s, may get blocked")
	}
}

func (s *txnManagerSuite) TestCloseLoaderInput(c *check.C) {
	input := make(chan *Txn)
	dmls := []*DML{
		{Tp: UpdateDMLType},
		{Tp: InsertDMLType},
		{Tp: UpdateDMLType},
	}
	txn := &Txn{DMLs: dmls}
	txnManager := newTxnManager(1, input)
	output := txnManager.run()

	inputTxnInTime(c, input, txn, time.Millisecond)
	close(input)

	t := outputTxnInTime(c, output, time.Millisecond)
	txnManager.pop(t)
	c.Assert(t, check.DeepEquals, txn)

	// output should be closed when input is closed
	select {
	case _, ok := <-output:
		c.Assert(ok, check.Equals, false)
	case <-time.After(time.Second):
		c.Fatal("txnManager fails to end run()... when input channel is closed")
	}
}

type runSuite struct{}

var _ = check.Suite(&runSuite{})

func (s *runSuite) TestShouldExecuteAllPendingDMLsOnClose(c *check.C) {
	var executed []*DML
	var cbTxns []*Txn
	origF := fNewBatchManager
	fNewBatchManager = func(s *loaderImpl) *batchManager {
		return &batchManager{
			limit:          1024,
			enableDispatch: true,
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
	executed := make(chan []*DML, 2)
	origF := fNewBatchManager
	fNewBatchManager = func(s *loaderImpl) *batchManager {
		return &batchManager{
			limit:          1024,
			enableDispatch: true,
			fExecDMLs: func(dmls []*DML) error {
				executed <- dmls
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

	assertExecuted := func(n int) {
		for i := 0; i < n; i++ {
			addTxn(i)
		}
		select {
		case dmls := <-executed:
			c.Assert(dmls, check.HasLen, n)
		case <-time.After(time.Second):
			c.Fatal("Timeout waiting to be executed.")
		}
	}

	assertExecuted(3)
	assertExecuted(7)
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
