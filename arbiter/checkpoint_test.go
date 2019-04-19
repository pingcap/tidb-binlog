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

package arbiter

import (
	"fmt"
	"testing"

	gosql "database/sql"
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	check "github.com/pingcap/check"
	"github.com/pingcap/errors"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
)

func Test(t *testing.T) { check.TestingT(t) }

type CheckpointSuite struct {
}

var _ = check.Suite(&CheckpointSuite{})

func setNewExpect(mock sqlmock.Sqlmock) {
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 1))
}

func (cs *CheckpointSuite) TestNewCheckpoint(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	setNewExpect(mock)

	_, err = createDbCheckpoint(db)
	c.Assert(err, check.IsNil)

	c.Assert(mock.ExpectationsWereMet(), check.IsNil)
}

func (cs *CheckpointSuite) TestSaveAndLoad(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	setNewExpect(mock)
	cp, err := createDbCheckpoint(db)
	c.Assert(err, check.IsNil)
	sql := fmt.Sprintf("SELECT (.+) FROM %s WHERE topic_name = ?",
		pkgsql.QuoteSchema(cp.database, cp.table))
	mock.ExpectQuery(sql).WithArgs(cp.topicName).
		WillReturnError(errors.NotFoundf("no checkpoint for: %s", cp.topicName))

	_, _, err = cp.Load()
	c.Log(err)
	c.Assert(errors.IsNotFound(err), check.IsTrue)

	var saveTS int64 = 10
	saveStatus := 1
	mock.ExpectExec("REPLACE INTO").
		WithArgs(cp.topicName, saveTS, saveStatus).
		WillReturnResult(sqlmock.NewResult(0, 1))
	err = cp.Save(saveTS, saveStatus)
	c.Assert(err, check.IsNil)

	rows := sqlmock.NewRows([]string{"ts", "status"}).
		AddRow(saveTS, saveStatus)
	mock.ExpectQuery("SELECT ts, status FROM").WillReturnRows(rows)
	ts, status, err := cp.Load()
	c.Assert(err, check.IsNil)
	c.Assert(ts, check.Equals, saveTS)
	c.Assert(status, check.Equals, saveStatus)
}


func createDbCheckpoint(db *gosql.DB) (*dbCheckpoint, error) {
	cp, err := NewCheckpoint(db, "topic_name")
	return cp.(*dbCheckpoint), err
}