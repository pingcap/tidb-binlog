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
	"database/sql"
	"fmt"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

type testNewServerSuite struct {
	db     *sql.DB
	dbMock sqlmock.Sqlmock
}

var _ = Suite(&testNewServerSuite{})

func (s *testNewServerSuite) SetUpTest(c *C) {
	db, mock, err := sqlmock.New()
	if err != nil {
		c.Fatalf("Failed to create mock db: %s", err)
	}
	s.db = db
	s.dbMock = mock
}

func (s *testNewServerSuite) TearDownTest(c *C) {
	s.db.Close()
}

func (s *testNewServerSuite) TestRejectInvalidAddr(c *C) {
	cfg := Config{ListenAddr: "whatever"}
	_, err := NewServer(&cfg)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*wrong ListenAddr.*")

	cfg.ListenAddr = "whatever:invalid"
	_, err = NewServer(&cfg)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "ListenAddr.*")
}

func (s *testNewServerSuite) TestStopIfFailedtoConnectDownStream(c *C) {
	origCreateDB := createDB
	createDB = func(user string, password string, host string, port int) (*sql.DB, error) {
		return nil, fmt.Errorf("Can't create db")
	}
	defer func() { createDB = origCreateDB }()

	cfg := Config{ListenAddr: "localhost:8080"}
	_, err := NewServer(&cfg)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "Can't create db")
}

func (s *testNewServerSuite) TestStopIfCannotCreateCheckpoint(c *C) {
	origCreateDB := createDB
	s.dbMock.ExpectExec("CREATE DATABASE IF NOT EXISTS `tidb_binlog`").WillReturnError(
		fmt.Errorf("cannot create"))
	createDB = func(user string, password string, host string, port int) (*sql.DB, error) {
		return s.db, nil
	}
	defer func() { createDB = origCreateDB }()

	cfg := Config{ListenAddr: "localhost:8080"}
	_, err := NewServer(&cfg)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "cannot create")
}
