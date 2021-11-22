// Copyright 2021 PingCAP, Inc.
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
	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

var _ = Suite(&testOracleCheckPointSuite{})

type testOracleCheckPointSuite struct{}

func (t *testOracleCheckPointSuite) TestClose(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	mock.ExpectClose()
	cp := OracleCheckPoint{db: db}
	cp.Close()
	cp.Close()
	c.Assert(cp.closed, IsTrue)
}

type oracleSaveSuite struct{}

var _ = Suite(&oracleSaveSuite{})

func (s *oracleSaveSuite) TestShouldLoadFromDB(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	cp := OracleCheckPoint{
		db:     db,
		schema: "db",
		table:  "tbl",
		TsMap:  make(map[string]int64),
	}
	rows := sqlmock.NewRows([]string{"checkPoint"}).
		AddRow(`{"commitTS": 1024, "consistent": true, "ts-map": {"primary-ts": 2000, "secondary-ts": 1999}}`)
	mock.ExpectQuery("select checkPoint from db.tbl.*").WillReturnRows(rows)

	err = cp.Load()
	c.Assert(err, IsNil)
	c.Assert(cp.CommitTS, Equals, int64(1024))
	c.Assert(cp.ConsistentSaved, Equals, true)
	c.Assert(cp.TsMap["primary-ts"], Equals, int64(2000))
	c.Assert(cp.TsMap["secondary-ts"], Equals, int64(1999))
}

func (s *oracleSaveSuite) TestShouldUseInitialCommitTs(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	cp := OracleCheckPoint{
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

type newOracleSuite struct{}

var _ = Suite(&newOracleSuite{})

func (s *newOracleSuite) TestServiceNameEmptyError(c *C) {
	_, err := newOracle(&Config{})
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*service-name should not be empty.*")
}

func (s *newOracleSuite) TestCannotOpenDB(c *C) {
	origOpen := sqlOpenOracleDB
	defer func() { sqlOpenOracleDB = origOpen }()
	sqlOpenOracleDB = func(user string, password string, host string, port int, serviceName, connectString string) (*sql.DB, error) {
		return nil, errors.New("no db")
	}
	_, err := newOracle(&Config{
		Db: &DBConfig{OracleServiceName: "service-name"},
	})
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*no db.*")
}

func (s *newOracleSuite) TestCreationErrors(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	origOpen := sqlOpenOracleDB
	defer func() { sqlOpenOracleDB = origOpen }()
	sqlOpenOracleDB = func(user string, password string, host string, port int, serviceName, connectString string) (*sql.DB, error) {
		return db, nil
	}
	rows := sqlmock.NewRows([]string{"TABLE_NAME"})
	mock.ExpectQuery("select table_name.*").WillReturnRows(rows)
	mock.ExpectExec("create table tidb_binlog.checkpoint").WillReturnError(errors.New("create checkpoint failed"))

	_, err = newOracle(&Config{
		Db: &DBConfig{OracleServiceName: "service-name"},
	})
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*create checkpoint table failed.*")

	mock.ExpectQuery("select table_name.*").WillReturnError(errors.New("check table failed"))
	_, err = newOracle(&Config{
		Db: &DBConfig{OracleServiceName: "service-name"},
	})
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*exec failed, sql:.*")
}

func (s *newOracleSuite) TestCreationSuccessful(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	origOpen := sqlOpenOracleDB
	defer func() { sqlOpenOracleDB = origOpen }()
	sqlOpenOracleDB = func(user string, password string, host string, port int, serviceName, connectString string) (*sql.DB, error) {
		return db, nil
	}
	tableNameRow := sqlmock.NewRows([]string{"TABLE_NAME"}).AddRow("checkPoint")
	clusterIDRow := sqlmock.NewRows([]string{"clusterID"}).AddRow("12345")
	checkPointRow := sqlmock.NewRows([]string{"checkPoint"}).
		AddRow(`{"commitTS": 1024, "consistent": true, "ts-map": {"primary-ts": 2000, "secondary-ts": 1999}}`)
	mock.ExpectQuery("select table_name.*").WillReturnRows(tableNameRow)
	mock.ExpectQuery("select clusterID from.*").WillReturnRows(clusterIDRow)
	mock.ExpectQuery("select checkPoint from tidb_binlog.checkpoint.*").WillReturnRows(checkPointRow)

	cp, err := newOracle(&Config{
		Db: &DBConfig{OracleServiceName: "service-name"},
	})
	pcp := cp.(*OracleCheckPoint)
	c.Assert(err, IsNil)
	c.Assert(cp, NotNil)
	c.Assert(pcp.CommitTS, Equals, int64(1024))
	c.Assert(pcp.ConsistentSaved, Equals, true)
	c.Assert(pcp.TsMap["primary-ts"], Equals, int64(2000))
	c.Assert(pcp.TsMap["secondary-ts"], Equals, int64(1999))
}
