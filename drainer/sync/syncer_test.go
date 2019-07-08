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
	"database/sql"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/drainer/translator"
)

func TestClient(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&syncerSuite{})

type syncerSuite struct {
	syncers []Syncer

	mysqlMock    sqlmock.Sqlmock
	flashMock    sqlmock.Sqlmock
	mockProducer *mocks.AsyncProducer
}

func (s *syncerSuite) SetUpTest(c *check.C) {
	var infoGetter translator.TableInfoGetter
	cfg := &DBConfig{
		Host:         "localhost",
		User:         "root",
		Password:     "",
		Port:         3306,
		KafkaVersion: "0.8.2.0",
		TimeLimit:    "1s",
		SizeLimit:    "1024",
	}

	// create pb syncer
	pb, err := NewPBSyncer(c.MkDir(), infoGetter)
	c.Assert(err, check.IsNil)

	s.syncers = append(s.syncers, pb)

	// create mysql syncer
	oldCreateDB := createDB
	createDB = func(string, string, string, int, *string) (db *sql.DB, err error) {
		db, s.mysqlMock, err = sqlmock.New()
		return
	}
	defer func() {
		createDB = oldCreateDB
	}()

	mysql, err := NewMysqlSyncer(cfg, infoGetter, 1, 1, nil, nil, mysql)
	c.Assert(err, check.IsNil)
	s.syncers = append(s.syncers, mysql)

	// create kafka syncer
	oldNewAsyncProducer := newAsyncProducer
	defer func() {
		newAsyncProducer = oldNewAsyncProducer
	}()
	newAsyncProducer = func(addrs []string, config *sarama.Config) (producer sarama.AsyncProducer, err error) {
		s.mockProducer = mocks.NewAsyncProducer(c, config)
		return s.mockProducer, nil
	}

	kafka, err := NewKafka(cfg, infoGetter)
	c.Assert(err, check.IsNil)
	s.syncers = append(s.syncers, kafka)

	// create flash
	oldOpenCH := openCH
	defer func() {
		openCH = oldOpenCH
	}()
	openCH = func(string, int, string, string, string, int) (db *sql.DB, err error) {
		db, s.flashMock, err = sqlmock.New()
		return
	}

	// flash does not use `cfg.Port`, and use `cfg.Host` as an `addr`
	cfg.Host += ":" + strconv.Itoa(cfg.Port)
	flash, err := NewFlashSyncer(cfg, infoGetter)
	c.Assert(err, check.IsNil)
	s.syncers = append(s.syncers, flash)

	c.Logf("set up %d syncer", len(s.syncers))
}

func (s *syncerSuite) TearDownTest(c *check.C) {
	s.flashMock.ExpectClose()
	s.mysqlMock.ExpectClose()

	closeSyncers(c, s.syncers)
	s.syncers = nil
}

func (s *syncerSuite) TestOpenAndClose(c *check.C) {
	// Test just thinks in `SetUpTest` and `TearDownTest` for just new and close syncer.
}

func (s *syncerSuite) TestGetFromSuccesses(c *check.C) {
	gen := translator.BinlogGenrator{}
	gen.SetDDL()
	item := &Item{
		Binlog:        gen.TiBinlog,
		PrewriteValue: gen.PV,
		Schema:        gen.Schema,
		Table:         gen.Table,
	}

	// set up mysql db mock expect
	s.mysqlMock.ExpectBegin()
	s.mysqlMock.ExpectExec("use .*").WillReturnResult(sqlmock.NewResult(0, 0))
	s.mysqlMock.ExpectExec("create table .*").WillReturnResult(sqlmock.NewResult(0, 0))
	s.mysqlMock.ExpectCommit()

	// set up kafka producer mock expect
	s.mockProducer.ExpectInputAndSucceed()

	// set up flash db mock expect
	s.flashMock.ExpectBegin()
	s.flashMock.ExpectExec("CREATE TABLE .*").WillReturnResult(sqlmock.NewResult(0, 0))
	s.flashMock.ExpectCommit()

	var successCount = make([]int64, len(s.syncers))
	for idx, syncer := range s.syncers {
		go func(idx int) {
			for range syncer.Successes() {
				atomic.AddInt64(&successCount[idx], 1)
			}
		}(idx)

		err := syncer.Sync(item)
		c.Assert(err, check.IsNil)

		// check we can get from Successes()
		tryNum := 10
		i := 0
		for ; i < tryNum; i++ {
			if atomic.LoadInt64(&successCount[idx]) == 1 {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}
		if i == tryNum {
			c.Logf("fail to get from  %v", reflect.TypeOf(syncer))
			c.FailNow()
		}

		c.Logf("success to get from  %v", reflect.TypeOf(syncer))
	}
}

func closeSyncers(c *check.C, syncers []Syncer) {
	for _, syncer := range syncers {
		err := syncer.Close()
		c.Assert(err, check.IsNil)

		c.Logf("close %T success", syncer)
	}
}
