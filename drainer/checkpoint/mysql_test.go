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

package checkpoint

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testCheckPointSuite{})

type testCheckPointSuite struct{}

func (t *testCheckPointSuite) TestClose(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	mock.ExpectClose()
	cp := MysqlCheckPoint{db: db}
	cp.Close()
	cp.Close()
	c.Assert(cp.closed, IsTrue)
}

type saveSuite struct{}

var _ = Suite(&saveSuite{})

func (s *saveSuite) TestShouldSaveCheckpoint(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	mock.ExpectExec("replace into db.tbl.*").WillReturnResult(sqlmock.NewResult(0, 0))
	cp := MysqlCheckPoint{db: db, schema: "db", table: "tbl"}
	err = cp.Save(1111, 0, false)
	c.Assert(err, IsNil)
}

func (s *saveSuite) TestShouldUpdateTsMap(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	cp := MysqlCheckPoint{
		db:     db,
		schema: "db",
		table:  "tbl",
		TsMap:  make(map[string]int64),
	}
	err = cp.Save(65536, 3333, false)
	c.Assert(err, IsNil)
	c.Assert(cp.TsMap["master-ts"], Equals, int64(65536))
	c.Assert(cp.TsMap["slave-ts"], Equals, int64(3333))
}

type loadSuite struct{}

var _ = Suite(&loadSuite{})

func (s *loadSuite) TestShouldLoadFromDB(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	cp := MysqlCheckPoint{
		db:     db,
		schema: "db",
		table:  "tbl",
		TsMap:  make(map[string]int64),
	}
	rows := sqlmock.NewRows([]string{"checkPoint"}).
		AddRow(`{"commitTS": 1024, "consistent": true, "ts-map": {"master-ts": 2000, "slave-ts": 1999}}`)
	mock.ExpectQuery("select checkPoint from db.tbl.*").WillReturnRows(rows)

	err = cp.Load()
	c.Assert(err, IsNil)
	c.Assert(cp.CommitTS, Equals, int64(1024))
	c.Assert(cp.ConsistentSaved, Equals, true)
	c.Assert(cp.TsMap["master-ts"], Equals, int64(2000))
	c.Assert(cp.TsMap["slave-ts"], Equals, int64(1999))
}

func (s *loadSuite) TestShouldUseInitialCommitTs(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	cp := MysqlCheckPoint{
		db:              db,
		schema:          "db",
		table:           "tbl",
		TsMap:           make(map[string]int64),
		initialCommitTS: 42,
	}
	mock.ExpectQuery(".*").WillReturnError(errors.New("test"))
	err = cp.Load()
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*test.*")
	c.Assert(cp.CommitTS, Equals, cp.initialCommitTS)
}

type newMysqlSuite struct{}

var _ = Suite(&newMysqlSuite{})

func (s *newMysqlSuite) TestCannotOpenDB(c *C) {
	origOpen := sqlOpenDB
	defer func() { sqlOpenDB = origOpen }()
	sqlOpenDB = func(proto, host string, port int, username, password string) (*sql.DB, error) {
		return nil, errors.New("no db")
	}

	_, err := newMysql(&Config{})
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*no db.*")
}

func (s *newMysqlSuite) TestCreationErrors(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	origOpen := sqlOpenDB
	defer func() { sqlOpenDB = origOpen }()
	sqlOpenDB = func(proto, host string, port int, username, password string) (*sql.DB, error) {
		return db, nil
	}

	mock.ExpectExec("create schema.*").WillReturnError(errors.New("fail schema"))
	_, err = newMysql(&Config{})
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*fail schema.*")

	mock.ExpectExec("create schema.*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("create table.*").WillReturnError(errors.New("fail table"))

	_, err = newMysql(&Config{})
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*fail table.*")
}
