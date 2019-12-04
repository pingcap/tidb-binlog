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
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	"github.com/pingcap/tidb-tools/tidb-binlog/driver/reader"
	pb "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
)

type dummyLoader struct {
	loader.Loader
	successes chan *loader.Txn
	safe      bool
	input     chan *loader.Txn
	closed    bool
}

func (l *dummyLoader) SetSafeMode(safe bool) {
	l.safe = safe
}

func (l *dummyLoader) Successes() <-chan *loader.Txn {
	return l.successes
}

func (l *dummyLoader) Input() chan<- *loader.Txn {
	return l.input
}

func (l *dummyLoader) Close() {
	l.closed = true
}

type testNewServerSuite struct {
	db            *sql.DB
	dbMock        sqlmock.Sqlmock
	origCreateDB  func(string, string, string, int) (*sql.DB, error)
	origNewReader func(*reader.Config) (*reader.Reader, error)
	origNewLoader func(*sql.DB, ...loader.Option) (loader.Loader, error)
}

var _ = Suite(&testNewServerSuite{})

func (s *testNewServerSuite) SetUpTest(c *C) {
	db, mock, err := sqlmock.New()
	if err != nil {
		c.Fatalf("Failed to create mock db: %s", err)
	}
	s.db = db
	s.dbMock = mock

	s.origCreateDB = createDB
	createDB = func(user string, password string, host string, port int) (*sql.DB, error) {
		return s.db, nil
	}

	s.origNewReader = newReader
	newReader = func(cfg *reader.Config) (r *reader.Reader, err error) {
		return &reader.Reader{}, nil
	}

	s.origNewLoader = newLoader
	newLoader = func(db *sql.DB, opt ...loader.Option) (loader.Loader, error) {
		return &dummyLoader{}, nil
	}
}

func (s *testNewServerSuite) TearDownTest(c *C) {
	s.db.Close()

	createDB = s.origCreateDB
	newReader = s.origNewReader
	newLoader = s.origNewLoader
}

func (s *testNewServerSuite) TestRejectInvalidAddr(c *C) {
	cfg := Config{ListenAddr: "whatever"}
	_, err := NewServer(&cfg)
	c.Assert(err, ErrorMatches, ".*wrong ListenAddr.*")

	cfg.ListenAddr = "whatever:invalid"
	_, err = NewServer(&cfg)
	c.Assert(err, ErrorMatches, "ListenAddr.*")
}

func (s *testNewServerSuite) TestStopIfFailedtoConnectDownStream(c *C) {
	createDB = func(user string, password string, host string, port int) (*sql.DB, error) {
		return nil, fmt.Errorf("Can't create db")
	}

	cfg := Config{ListenAddr: "localhost:8080"}
	_, err := NewServer(&cfg)
	c.Assert(err, ErrorMatches, "Can't create db")
}

func (s *testNewServerSuite) TestStopIfCannotCreateCheckpoint(c *C) {
	s.dbMock.ExpectExec("CREATE DATABASE IF NOT EXISTS `tidb_binlog`").WillReturnError(
		fmt.Errorf("cannot create"))

	cfg := Config{ListenAddr: "localhost:8080"}
	_, err := NewServer(&cfg)
	c.Assert(err, ErrorMatches, "cannot create")
}

func (s *testNewServerSuite) TestStopIfCannotLoadStatus(c *C) {
	s.dbMock.ExpectExec("CREATE DATABASE.*").WillReturnResult(sqlmock.NewResult(0, 0))
	s.dbMock.ExpectExec("CREATE TABLE.*").WillReturnResult(sqlmock.NewResult(0, 0))
	s.dbMock.ExpectQuery("SELECT ts, status.*").
		WithArgs("test_topic").
		WillReturnError(errors.New("Failed load"))

	cfg := Config{
		ListenAddr: "localhost:8080",
		Up: UpConfig{
			Topic: "test_topic",
		},
	}
	_, err := NewServer(&cfg)
	c.Assert(err, ErrorMatches, "Failed load")
}

func (s *testNewServerSuite) TestStopIfCannotCreateReader(c *C) {
	s.dbMock.ExpectExec("CREATE DATABASE.*").WillReturnResult(sqlmock.NewResult(0, 0))
	s.dbMock.ExpectExec("CREATE TABLE.*").WillReturnResult(sqlmock.NewResult(0, 0))
	s.dbMock.ExpectQuery("SELECT ts, status.*").
		WithArgs("test_topic").
		WillReturnError(errors.NotFoundf(""))
	newReader = func(cfg *reader.Config) (r *reader.Reader, err error) {
		return nil, errors.New("no reader")
	}

	cfg := Config{
		ListenAddr: "localhost:8080",
		Up: UpConfig{
			Topic: "test_topic",
		},
	}
	_, err := NewServer(&cfg)
	c.Assert(err, ErrorMatches, "no reader")
}

func (s *testNewServerSuite) TestStopIfCannotCreateLoader(c *C) {
	s.dbMock.ExpectExec("CREATE DATABASE.*").WillReturnResult(sqlmock.NewResult(0, 0))
	s.dbMock.ExpectExec("CREATE TABLE.*").WillReturnResult(sqlmock.NewResult(0, 0))
	s.dbMock.ExpectQuery("SELECT ts, status.*").
		WithArgs("test_topic").
		WillReturnError(errors.New("not found"))
	newLoader = func(db *sql.DB, opt ...loader.Option) (loader.Loader, error) {
		return nil, errors.New("no loader")
	}

	cfg := Config{
		ListenAddr: "localhost:8080",
		Up: UpConfig{
			Topic: "test_topic",
		},
	}
	_, err := NewServer(&cfg)
	c.Assert(err, ErrorMatches, "no loader")
}

func (s *testNewServerSuite) TestSetSafeMode(c *C) {
	s.dbMock.ExpectExec("CREATE DATABASE.*").WillReturnResult(sqlmock.NewResult(0, 0))
	s.dbMock.ExpectExec("CREATE TABLE.*").WillReturnResult(sqlmock.NewResult(0, 0))
	rows := sqlmock.NewRows([]string{"ts", "status"}).AddRow(42, StatusRunning)
	s.dbMock.ExpectQuery("SELECT ts, status.*").
		WithArgs("test_topic").
		WillReturnRows(rows)
	var ld dummyLoader
	newLoader = func(db *sql.DB, opt ...loader.Option) (loader.Loader, error) {
		return &ld, nil
	}

	origDuration := initSafeModeDuration
	defer func() {
		initSafeModeDuration = origDuration
	}()
	initSafeModeDuration = 10 * time.Millisecond

	cfg := Config{
		ListenAddr: "localhost:8080",
		Up: UpConfig{
			Topic: "test_topic",
		},
	}
	_, err := NewServer(&cfg)
	c.Assert(err, IsNil)
	c.Assert(ld.safe, IsTrue)
	time.Sleep(2 * initSafeModeDuration)
	c.Assert(ld.safe, IsFalse)
}

func (s *testNewServerSuite) TestCreateMetricCli(c *C) {
	s.dbMock.ExpectExec("CREATE DATABASE.*").WillReturnResult(sqlmock.NewResult(0, 0))
	s.dbMock.ExpectExec("CREATE TABLE.*").WillReturnResult(sqlmock.NewResult(0, 0))
	s.dbMock.ExpectQuery("SELECT ts, status.*").
		WithArgs("test_topic").
		WillReturnError(errors.New("not found"))

	cfg := Config{
		ListenAddr: "localhost:8080",
		Up: UpConfig{
			Topic: "test_topic",
		},
		Metrics: Metrics{
			Addr:     "testing",
			Interval: 10,
		},
	}
	srv, err := NewServer(&cfg)
	c.Assert(err, IsNil)
	c.Assert(srv.metrics, NotNil)
}

type updateFinishTSSuite struct{}

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
	status     []int
}

func (cp *dummyCp) Save(ts int64, status int) error {
	cp.timestamps = append(cp.timestamps, ts)
	cp.status = append(cp.status, status)
	return nil
}

func (s *trackTSSuite) TestShouldUpdateFinishTS(c *C) {
	cp := dummyCp{}
	successes := make(chan *loader.Txn, 1)
	ld := dummyLoader{successes: successes}
	server := Server{
		load:       &ld,
		checkpoint: &cp,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		server.trackTS(context.Background(), 50*time.Millisecond)
		wg.Done()
	}()

	for i := 0; i < 42; i++ {
		successes <- &loader.Txn{Metadata: &reader.Message{Binlog: &pb.Binlog{CommitTs: int64(i)}}}
	}
	close(successes)

	wg.Wait()
	c.Assert(server.finishTS, Equals, int64(41))
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
		load:       ld,
		checkpoint: &cp,
	}

	ctx, cancel := context.WithCancel(context.Background())

	stop := make(chan struct{})
	go func() {
		server.trackTS(ctx, 50*time.Millisecond)
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
	ts     int64
	status int
	err    error
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

type syncBinlogsSuite struct{}

var _ = Suite(&syncBinlogsSuite{})

func (s *syncBinlogsSuite) createMsg(schema, table, sql string, commitTs int64) *reader.Message {
	return &reader.Message{
		Binlog: &pb.Binlog{
			DdlData: &pb.DDLData{
				SchemaName: &schema,
				TableName:  &table,
				DdlQuery:   []byte(sql),
			},
			CommitTs: commitTs,
		},
	}
}

func (s *syncBinlogsSuite) TestShouldSendBinlogToLoader(c *C) {
	source := make(chan *reader.Message, 1)
	msgs := []*reader.Message{
		s.createMsg("test42", "users", "alter table users add column gender smallint", 1),
		s.createMsg("test42", "users", "alter table users add column gender smallint", 1),
		s.createMsg("test42", "operations", "alter table operations drop column seq", 2),
		s.createMsg("test42", "users", "alter table users add column gender smallint", 1),
		s.createMsg("test42", "operations", "alter table operations drop column seq", 2),
	}
	expectMsgs := []*reader.Message{
		s.createMsg("test42", "users", "alter table users add column gender smallint", 1),
		s.createMsg("test42", "operations", "alter table operations drop column seq", 2),
	}
	dest := make(chan *loader.Txn, len(msgs))
	go func() {
		for _, m := range msgs {
			source <- m
		}
		close(source)
	}()
	ld := dummyLoader{input: dest}

	err := syncBinlogs(context.Background(), source, &ld)
	c.Assert(err, IsNil)

	c.Assert(len(dest), Equals, len(expectMsgs))
	for _, m := range expectMsgs {
		txn := <-dest
		c.Assert(txn.Metadata.(*reader.Message), DeepEquals, m)
	}

	c.Assert(ld.closed, IsTrue)
}

func (s *syncBinlogsSuite) TestShouldQuitWhenSomeErrorOccurs(c *C) {
	readerMsgs := make(chan *reader.Message, 1024)
	dummyLoaderImpl := &dummyLoader{
		successes: make(chan *loader.Txn),
		// input is set small to trigger blocking easily
		input: make(chan *loader.Txn, 1),
	}
	msg := s.createMsg("test42", "users", "alter table users add column gender smallint", 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// start a routine keep sending msgs to kafka reader
	go func() {
		// make sure there will be some msgs in readerMsgs for test
		for i := 0; i < 3; i++ {
			readerMsgs <- msg
		}
		defer close(readerMsgs)
		for {
			select {
			case <-ctx.Done():
				return
			case readerMsgs <- msg:
			}
		}
	}()
	errCh := make(chan error)
	go func() {
		errCh <- syncBinlogs(ctx, readerMsgs, dummyLoaderImpl)
	}()

	cancel()
	select {
	case err := <-errCh:
		c.Assert(err, IsNil)
	case <-time.After(time.Second):
		c.Fatal("server doesn't quit in 1s when some error occurs in loader")
	}
}
