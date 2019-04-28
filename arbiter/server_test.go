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
	"time"
	"database/sql"
	"context"
	"fmt"

	"github.com/pingcap/errors"
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	"github.com/pingcap/tidb-tools/tidb-binlog/driver/reader"
	pb "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
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

type updateFinishTSSuite struct {}

var _ = Suite(&updateFinishTSSuite{})

func (s *updateFinishTSSuite) TestShouldSetFinishTS(c *C) {
	server := Server{}	
	msg := reader.Message{
		Binlog: &pb.Binlog{
			CommitTs: 1024,		
		},
	}
	c.Assert(server.finishTS, Equals, int64(0))
	server.updateFinishTS(&msg)
	c.Assert(server.finishTS, Equals, int64(1024))
}

type trackTSSuite struct{}

var _ = Suite(&trackTSSuite{})

type dummyCp struct {
	Checkpoint
	timestamps []int64
	status []int
}

func (cp *dummyCp) Save(ts int64, status int) error {
	cp.timestamps = append(cp.timestamps, ts)
	cp.status = append(cp.status, status)
	return nil
}

func (s *trackTSSuite) TestShouldSaveFinishTS(c *C) {
	db, _, err := sqlmock.New()
	if err != nil {
		c.Fatalf("Failed to create mock db: %s", err)
	}
	ld, err := loader.NewLoader(db)
	c.Assert(err, IsNil)
	cp := dummyCp{}
	server := Server{
		load: ld,
		checkpoint: &cp,
	}	

	ctx, cancel := context.WithCancel(context.Background())

	stop := make(chan struct{})
	go func() {
		server.trackTS(ctx, 50 * time.Millisecond)
		close(stop)
	}()

	for i := 0; i < 42; i++ {
		server.finishTS = int64(i)
		time.Sleep(2 * time.Millisecond)
	}

	cancel()

	select {
	case <-stop:
	case <-time.After(time.Second):
		c.Fatal("Doesn't stop in time")
	}

	c.Assert(len(cp.status), Greater, 1)
	c.Assert(len(cp.timestamps), Greater, 1)
	c.Assert(cp.status[len(cp.status)-1], Equals, StatusRunning)
	c.Assert(cp.timestamps[len(cp.timestamps)-1], Equals, int64(41))
}

type loadStatusSuite struct{}

var _ = Suite(&loadStatusSuite{})

type configurableCp struct {
	Checkpoint
	ts int64
	status int
	err error
}

func (c *configurableCp) Load() (ts int64, status int, err error) {
	return c.ts, c.status, c.err
}

func (s *loadStatusSuite) TestShouldIgnoreNotFound(c *C) {
	cp := configurableCp{status: StatusNormal, err: errors.NotFoundf("")}
	server := Server{
		checkpoint: &cp,
	}
	status, err := server.loadStatus()
	c.Assert(err, IsNil)
	c.Assert(status, Equals, cp.status)
}

func (s *loadStatusSuite) TestShouldSetFinishTS(c *C) {
	cp := configurableCp{status: StatusRunning, ts: 1984}
	server := Server{
		checkpoint: &cp,
	}
	status, err := server.loadStatus()
	c.Assert(err, IsNil)
	c.Assert(status, Equals, cp.status)
	c.Assert(server.finishTS, Equals, cp.ts)
}

func (s *loadStatusSuite) TestShouldRetErr(c *C) {
	cp := configurableCp{status: StatusNormal, err: errors.New("other")}
	server := Server{
		checkpoint: &cp,
	}
	_, err := server.loadStatus()
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "other")
}