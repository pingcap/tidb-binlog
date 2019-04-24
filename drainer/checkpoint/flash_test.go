// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the
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
	"errors"
	"regexp"
	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

type flashSuite struct{}

var _ = Suite(&flashSuite{})

func (s *flashSuite) TestcheckFlashConfig(c *C) {
	cfg := Config{}
	checkFlashConfig(&cfg)
	c.Assert(cfg.Db.Host, Equals, "127.0.0.1")
	c.Assert(cfg.Db.Port, Equals, 9000)
	c.Assert(cfg.Schema, Equals, "tidb_binlog")
	c.Assert(cfg.Table, Equals, "checkpoint")
}

func (s *flashSuite) TestFlashCheckPointString(c *C) {
	cp := FlashCheckPoint{CommitTS: 1234}
	c.Assert(cp.String(), Equals, "binlog commitTS = 1234")
}

type newFlashSuite struct{}

var _ = Suite(&newFlashSuite{})

func (s *newFlashSuite) TestShouldRejectInvalidHost(c *C) {
	cfg := Config{Db: &DBConfig{Host: "invalid"}}
	_, err := newFlash(&cfg)
	c.Assert(err, NotNil)
}

func (s *newFlashSuite) TestCannotOpenDB(c *C) {
	origOpen := openCH
	openCH = func(host string, port int, username string, password string, dbName string, blockSize int) (*sql.DB, error) {
		return nil, errors.New("OpenErr")
	}
	defer func ()  {
		openCH = origOpen	
	}()
	cfg := Config{Db: &DBConfig{Host: "127.0.0.1:9000"}}
	_, err := newFlash(&cfg)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "OpenErr")
}

func (s *newFlashSuite) TestDBStatementErrs(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	origOpen := openCH
	openCH = func(host string, port int, username string, password string, dbName string, blockSize int) (*sql.DB, error) {
		return db, nil
	}
	defer func ()  {
		openCH = origOpen	
	}()

	sqlDB := "CREATE DATABASE IF NOT EXISTS `test`"
	mock.ExpectExec(sqlDB).WillReturnError(errors.New("createdb"))

	cfg := Config{Db: &DBConfig{Host: "127.0.0.1:9000"}, Schema: "test", Table: "tbl"}
	_, err = newFlash(&cfg)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "createdb")

	mock.ExpectExec(sqlDB).WillReturnResult(sqlmock.NewResult(0, 0))
	sqlAttach := regexp.QuoteMeta("ATTACH TABLE IF NOT EXISTS `test`.`tbl`(`clusterid` UInt64, `checkpoint` String) ENGINE MutableMergeTree((`clusterid`), 8192)")
	mock.ExpectExec(sqlAttach).WillReturnError(errors.New("attachtbl"))
	_, err = newFlash(&cfg)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "attachtbl")
}