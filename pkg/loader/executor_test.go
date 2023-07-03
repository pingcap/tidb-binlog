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
	"fmt"
	"regexp"
	"sync/atomic"

	"github.com/pingcap/tidb/parser/model"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/check"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type executorSuite struct{}

var _ = Suite(&executorSuite{})

func (s *executorSuite) TestNewExecutor(c *C) {
	db, _, err := sqlmock.New()
	c.Assert(err, IsNil)

	var e *executor = newExecutor(db).withBatchSize(37).
		withQueryHistogramVec(&prometheus.HistogramVec{})
	c.Assert(e.db, NotNil)
	c.Assert(e.batchSize, Equals, 37)
	c.Assert(e.queryHistogramVec, NotNil)
}

func (s *executorSuite) TestSplitExecDML(c *C) {
	var dmls []*DML
	for i := 0; i < 5; i++ {
		dml := DML{
			Database: "unicorn",
			Table:    "users",
			Tp:       InsertDMLType,
			Values: map[string]interface{}{
				"name": fmt.Sprintf("tester%d", i),
			},
			info: &tableInfo{
				columns: []string{"name"},
			},
		}
		dmls = append(dmls, &dml)
	}

	db, _, err := sqlmock.New()
	c.Assert(err, IsNil)
	e := newExecutor(db).withBatchSize(2)

	var counter int32

	err = e.splitExecDML(context.Background(), dmls, func(group []*DML) error {
		atomic.AddInt32(&counter, 1)
		if len(group) < 2 {
			return errors.New("fake")
		}
		return nil
	})
	c.Assert(err, ErrorMatches, "fake")
	c.Assert(counter, Equals, int32(3))
}

func (s *executorSuite) TestTryRefreshTableErr(c *C) {
	tests := []struct {
		err error
		res bool
	}{
		{&mysql.MySQLError{Number: 1054} /*Unknown column*/, true},
		{errors.New("what ever"), false},
	}

	for _, test := range tests {
		get := tryRefreshTableErr(test.err)
		c.Assert(get, check.Equals, test.res)
	}
}

type singleExecSuite struct {
	db     *sql.DB
	dbMock sqlmock.Sqlmock
}

var _ = Suite(&singleExecSuite{})

func (s *singleExecSuite) SetUpTest(c *C) {
	s.resetMock(c)
}

func (s *singleExecSuite) resetMock(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	s.db = db
	s.dbMock = mock
}

func (s *singleExecSuite) TestFailedToBeginTx(c *C) {
	s.dbMock.ExpectBegin().WillReturnError(errors.New("begin"))
	e := newExecutor(s.db)
	err := e.singleExec([]*DML{}, true)
	c.Assert(err, ErrorMatches, "begin")
	c.Assert(s.dbMock.ExpectationsWereMet(), IsNil)
}

func (s *singleExecSuite) TestInsert(c *C) {
	dml := DML{
		Database: "unicorn",
		Table:    "users",
		Tp:       InsertDMLType,
		Values: map[string]interface{}{
			"name": "tester",
			"age":  2019,
		},
		info: &tableInfo{
			columns: []string{"name", "age"},
		},
	}
	insertSQL := "INSERT INTO `unicorn`.`users`(`age`,`name`) VALUES(?,?)"
	replaceSQL := "REPLACE INTO `unicorn`.`users`(`age`,`name`) VALUES(?,?)"

	s.dbMock.ExpectBegin()
	s.dbMock.ExpectExec(regexp.QuoteMeta(insertSQL)).
		WithArgs(2019, "tester").WillReturnResult(sqlmock.NewResult(1, 1))
	s.dbMock.ExpectCommit()

	e := newExecutor(s.db)
	err := e.singleExec([]*DML{&dml}, false)
	c.Assert(err, IsNil)
	c.Assert(s.dbMock.ExpectationsWereMet(), IsNil)

	s.resetMock(c)

	s.dbMock.ExpectBegin()
	s.dbMock.ExpectExec(regexp.QuoteMeta(replaceSQL)).
		WithArgs(2019, "tester").WillReturnResult(sqlmock.NewResult(1, 1))
	s.dbMock.ExpectCommit()

	e = newExecutor(s.db)
	err = e.singleExec([]*DML{&dml}, true)
	c.Assert(err, IsNil)
	c.Assert(s.dbMock.ExpectationsWereMet(), IsNil)
}

func (s *singleExecSuite) TestSafeUpdate(c *C) {
	dml := DML{
		Database: "unicorn",
		Table:    "users",
		Tp:       UpdateDMLType,
		OldValues: map[string]interface{}{
			"name": "tester",
			"age":  1999,
		},
		Values: map[string]interface{}{
			"name": "tester",
			"age":  2019,
		},
		info: &tableInfo{
			columns: []string{"name", "age"},
			uniqueKeys: []indexInfo{
				{name: "name", columns: []string{"name"}},
			},
		},
	}
	delSQL := "DELETE FROM `unicorn`.`users`.*"
	replaceSQL := "REPLACE INTO `unicorn`.`users`.*"

	// When deleting failed
	s.dbMock.ExpectBegin()
	s.dbMock.ExpectExec(delSQL).
		WithArgs("tester").WillReturnError(errors.New("del"))

	e := newExecutor(s.db)
	err := e.singleExec([]*DML{&dml}, true)
	c.Assert(err, ErrorMatches, "del")
	c.Assert(s.dbMock.ExpectationsWereMet(), IsNil)

	s.resetMock(c)

	// When deleting succeed but replacing failed
	s.dbMock.ExpectBegin()
	s.dbMock.ExpectExec(delSQL).
		WithArgs("tester").WillReturnResult(sqlmock.NewResult(1, 1))
	s.dbMock.ExpectExec(replaceSQL).
		WithArgs(2019, "tester").WillReturnError(errors.New("replace"))

	e = newExecutor(s.db)
	err = e.singleExec([]*DML{&dml}, true)
	c.Assert(err, ErrorMatches, "replace")
	c.Assert(s.dbMock.ExpectationsWereMet(), IsNil)

	s.resetMock(c)

	s.dbMock.ExpectBegin()
	s.dbMock.ExpectExec(delSQL).
		WithArgs("tester").WillReturnResult(sqlmock.NewResult(1, 1))
	s.dbMock.ExpectExec(replaceSQL).
		WithArgs(2019, "tester").WillReturnResult(sqlmock.NewResult(1, 1))
	s.dbMock.ExpectCommit()

	e = newExecutor(s.db)
	err = e.singleExec([]*DML{&dml}, true)
	c.Assert(err, IsNil)
	c.Assert(s.dbMock.ExpectationsWereMet(), IsNil)
}

func (s *singleExecSuite) TestOracleSafeUpdate(c *C) {
	dml := DML{
		Database: "unicorn",
		Table:    "users",
		Tp:       UpdateDMLType,
		OldValues: map[string]interface{}{
			"name": "tester",
			"age":  1999,
		},
		Values: map[string]interface{}{
			"name": "tester_new",
			"age":  2019,
		},
		info: &tableInfo{
			columns: []string{"name", "age"},
			uniqueKeys: []indexInfo{
				{name: "name", columns: []string{"name"}},
			},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"name": {
				FieldType: *types.NewFieldType(tmysql.TypeString),
			},
			"age": {
				FieldType: *types.NewFieldType(tmysql.TypeInt24),
			},
		},
		DestDBType: OracleDB,
	}
	delSQL := "DELETE FROM unicorn.users.*"
	insertSQL := "INSERT INTO unicorn.users.*"
	// When firstly  deleting failed
	s.dbMock.ExpectBegin()
	s.dbMock.ExpectExec(delSQL).WillReturnError(errors.New("del"))

	e := newExecutor(s.db)
	e.destDBType = OracleDB
	err := e.singleOracleExec([]*DML{&dml}, true)
	c.Assert(err, ErrorMatches, "del")
	c.Assert(s.dbMock.ExpectationsWereMet(), IsNil)
	s.resetMock(c)

	// When the second deleting failed
	s.dbMock.ExpectBegin()
	s.dbMock.ExpectExec(delSQL).WillReturnResult(sqlmock.NewResult(1, 1))
	s.dbMock.ExpectExec(delSQL).WillReturnError(errors.New("del"))
	e = newExecutor(s.db)
	e.destDBType = OracleDB
	err = e.singleOracleExec([]*DML{&dml}, true)
	c.Assert(err, ErrorMatches, "del")
	c.Assert(s.dbMock.ExpectationsWereMet(), IsNil)
	s.resetMock(c)

	//when two deleting succeed, insert failed
	s.dbMock.ExpectBegin()
	s.dbMock.ExpectExec(delSQL).WillReturnResult(sqlmock.NewResult(1, 1))
	s.dbMock.ExpectExec(delSQL).WillReturnResult(sqlmock.NewResult(1, 1))
	s.dbMock.ExpectExec(insertSQL).WillReturnError(errors.New("insert"))
	e = newExecutor(s.db)
	err = e.singleOracleExec([]*DML{&dml}, true)
	e.destDBType = OracleDB
	c.Assert(err, ErrorMatches, "insert")
	c.Assert(s.dbMock.ExpectationsWereMet(), IsNil)
	s.resetMock(c)

	//all db operation successfully
	s.dbMock.ExpectBegin()
	s.dbMock.ExpectExec(delSQL).WillReturnResult(sqlmock.NewResult(1, 1))
	s.dbMock.ExpectExec(delSQL).WillReturnResult(sqlmock.NewResult(1, 1))
	s.dbMock.ExpectExec(insertSQL).WillReturnResult(sqlmock.NewResult(1, 1))
	s.dbMock.ExpectCommit()
	e = newExecutor(s.db)
	e.destDBType = OracleDB
	err = e.singleOracleExec([]*DML{&dml}, true)
	c.Assert(err, IsNil)
	c.Assert(s.dbMock.ExpectationsWereMet(), IsNil)

}

func (s *singleExecSuite) TestOracleSafeInsert(c *C) {
	dml := DML{
		Database: "unicorn",
		Table:    "users",
		Tp:       InsertDMLType,
		Values: map[string]interface{}{
			"name": "tester",
			"age":  2019,
		},
		info: &tableInfo{
			columns: []string{"name", "age"},
			uniqueKeys: []indexInfo{
				{name: "name", columns: []string{"name"}},
			},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"name": {
				FieldType: *types.NewFieldType(tmysql.TypeString),
			},
			"age": {
				FieldType: *types.NewFieldType(tmysql.TypeInt24),
			},
		},
		DestDBType: OracleDB,
	}
	delSQL := "DELETE FROM unicorn.users.*"
	insertSQL := "INSERT INTO unicorn.users.*"

	// When deleting failed
	s.dbMock.ExpectBegin()
	s.dbMock.ExpectExec(delSQL).WillReturnError(errors.New("del"))

	e := newExecutor(s.db)
	e.destDBType = OracleDB
	err := e.singleOracleExec([]*DML{&dml}, true)
	c.Assert(err, ErrorMatches, "del")
	c.Assert(s.dbMock.ExpectationsWereMet(), IsNil)
	s.resetMock(c)

	//When deleting succeed but insert failed
	s.dbMock.ExpectBegin()
	s.dbMock.ExpectExec(delSQL).WillReturnResult(sqlmock.NewResult(1, 1))
	s.dbMock.ExpectExec(insertSQL).WillReturnError(errors.New("insert"))
	e = newExecutor(s.db)
	e.destDBType = OracleDB
	err = e.singleOracleExec([]*DML{&dml}, true)
	c.Assert(err, ErrorMatches, "insert")
	c.Assert(s.dbMock.ExpectationsWereMet(), IsNil)
	s.resetMock(c)

	//all operation successfully
	s.dbMock.ExpectBegin()
	s.dbMock.ExpectExec(delSQL).WillReturnResult(sqlmock.NewResult(1, 1))
	s.dbMock.ExpectExec(insertSQL).WillReturnResult(sqlmock.NewResult(1, 1))
	s.dbMock.ExpectCommit()
	e = newExecutor(s.db)
	e.destDBType = OracleDB
	err = e.singleOracleExec([]*DML{&dml}, true)
	c.Assert(err, IsNil)
	c.Assert(s.dbMock.ExpectationsWereMet(), IsNil)
}

func (s *singleExecSuite) TestOracleSafeDelete(c *C) {
	dml := DML{
		Database: "unicorn",
		Table:    "users",
		Tp:       DeleteDMLType,
		Values: map[string]interface{}{
			"name": "tester",
			"age":  2019,
		},
		info: &tableInfo{
			columns: []string{"name", "age"},
			uniqueKeys: []indexInfo{
				{name: "name", columns: []string{"name"}},
			},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"name": {
				FieldType: *types.NewFieldType(tmysql.TypeString),
			},
			"age": {
				FieldType: *types.NewFieldType(tmysql.TypeInt24),
			},
		},
		DestDBType: OracleDB,
	}
	delSQL := "DELETE FROM unicorn.users.*"

	// When deleting failed
	s.dbMock.ExpectBegin()
	s.dbMock.ExpectExec(delSQL).WillReturnError(errors.New("del"))

	e := newExecutor(s.db)
	e.destDBType = OracleDB
	err := e.singleOracleExec([]*DML{&dml}, true)
	c.Assert(err, ErrorMatches, "del")
	c.Assert(s.dbMock.ExpectationsWereMet(), IsNil)
	s.resetMock(c)

	//all operation successfully
	s.dbMock.ExpectBegin()
	s.dbMock.ExpectExec(delSQL).WillReturnResult(sqlmock.NewResult(1, 1))
	s.dbMock.ExpectCommit()
	e = newExecutor(s.db)
	e.destDBType = OracleDB
	err = e.singleOracleExec([]*DML{&dml}, true)
	c.Assert(err, IsNil)
	c.Assert(s.dbMock.ExpectationsWereMet(), IsNil)
}

type bulkDelSuite struct{}

var _ = Suite(&bulkDelSuite{})

func (s *bulkDelSuite) TestCanHandleEmptySlice(c *C) {
	db, _, err := sqlmock.New()
	c.Assert(err, IsNil)

	e := newExecutor(db)
	err = e.bulkDelete([]*DML{})
	c.Assert(err, IsNil)
}

func (s *bulkDelSuite) TestDeleteInBulk(c *C) {
	var dmls []*DML
	for i := 0; i < 3; i++ {
		dml := DML{
			Database: "unicorn",
			Table:    "users",
			Tp:       DeleteDMLType,
			Values: map[string]interface{}{
				"name": fmt.Sprintf("tester_%d", i),
			},
			info: &tableInfo{
				columns: []string{"name"},
				uniqueKeys: []indexInfo{
					{name: "name", columns: []string{"name"}},
				},
			},
		}
		dmls = append(dmls, &dml)
	}

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mock.ExpectBegin()
	mock.ExpectExec("(DELETE FROM .*){3}").
		WithArgs("tester_0", "tester_1", "tester_2").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	e := newExecutor(db)
	err = e.bulkDelete(dmls)
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

type bulkReplaceSuite struct{}

var _ = Suite(&bulkReplaceSuite{})

func (s *bulkReplaceSuite) TestCanHandleEmptySlice(c *C) {
	db, _, err := sqlmock.New()
	c.Assert(err, IsNil)

	e := newExecutor(db)
	err = e.bulkReplace([]*DML{})
	c.Assert(err, IsNil)
}

func (s *bulkReplaceSuite) TestReplaceInBulk(c *C) {
	var dmls []*DML
	for i := 0; i < 3; i++ {
		dml := DML{
			Database: "d",
			Table:    "t",
			Tp:       InsertDMLType,
			Values: map[string]interface{}{
				"a": fmt.Sprintf("a_%d", i),
				"b": fmt.Sprintf("b_%d", i),
			},
			info: &tableInfo{
				columns: []string{"a", "b"},
			},
		}
		dmls = append(dmls, &dml)
	}

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mock.ExpectBegin()
	sql := "REPLACE INTO `d`.`t`(`a`,`b`) VALUES (?,?),(?,?),(?,?)"
	mock.ExpectExec(regexp.QuoteMeta(sql)).
		WithArgs("a_0", "b_0", "a_1", "b_1", "a_2", "b_2").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	e := newExecutor(db)
	err = e.bulkReplace(dmls)
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}
