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
	"fmt"

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

func (s *oracleSaveSuite) TestShouldSaveOracleCheckpoint(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	checkpointStr := `{"consistent":false,"commitTS":1111,"ts-map":null,"schema-version":0}`
	expecStr := fmt.Sprintf("merge into %s.%s t using \\(select %d clusterID, '%s' checkPoint from dual\\) temp on\\(t.clusterID=temp.clusterID\\) "+
		"when matched then "+
		"update set t.checkPoint=temp.checkPoint "+
		"when not matched then "+
		"insert values\\(temp.clusterID,temp.checkPoint\\)", "db", "tbl", 0, checkpointStr)
	//mock.ExpectExec("merge into db.tbl.*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(expecStr).WillReturnResult(sqlmock.NewResult(0, 0))
	cp := OracleCheckPoint{db: db, schema: "db", table: "tbl"}
	err = cp.Save(1111, 0, false, 0)
	c.Assert(err, IsNil)
}

func (s *oracleSaveSuite) TestShouldOracleUpdateTsMap(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	checkpointStr := `{"consistent":false,"commitTS":65536,"ts-map":{"primary-ts":65536,"secondary-ts":3333},"schema-version":0}`
	expecStr := fmt.Sprintf("merge into %s.%s t using \\(select %d clusterID, '%s' checkPoint from dual\\) temp on\\(t.clusterID=temp.clusterID\\) "+
		"when matched then "+
		"update set t.checkPoint=temp.checkPoint "+
		"when not matched then "+
		"insert values\\(temp.clusterID,temp.checkPoint\\)", "db", "tbl", 0, checkpointStr)
	mock.ExpectExec(expecStr).WillReturnResult(sqlmock.NewResult(0, 0))
	cp := OracleCheckPoint{
		db:     db,
		schema: "db",
		table:  "tbl",
		TsMap:  make(map[string]int64),
	}
	err = cp.Save(65536, 3333, false, 0)
	c.Assert(err, IsNil)
	c.Assert(cp.TsMap["primary-ts"], Equals, int64(65536))
	c.Assert(cp.TsMap["secondary-ts"], Equals, int64(3333))
}

func (s *oracleSaveSuite) TestShouldLoadFromDB(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	cp := OracleCheckPoint{
		db:     db,
		schema: "db",
		table:  "tbl",
		TsMap:  make(map[string]int64),
	}
	rows := sqlmock.NewRows([]string{"CHECKPOINT"}).
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
	mock.ExpectExec("create table.*").WillReturnError(errors.New("create checkpoint failed"))

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
	clusterIDRow := sqlmock.NewRows([]string{"CLUSTERID"}).AddRow("12345")
	checkPointRow := sqlmock.NewRows([]string{"CHECKPOINT"}).
		AddRow(`{"commitTS": 1024, "consistent": true, "ts-map": {"primary-ts": 2000, "secondary-ts": 1999}}`)
	mock.ExpectQuery("select table_name.*").WillReturnRows(tableNameRow)
	mock.ExpectQuery("select clusterID from.*").WillReturnRows(clusterIDRow)
	mock.ExpectQuery("select checkPoint from.*").WillReturnRows(checkPointRow)

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

func (s *newOracleSuite) TestDefaultCheckPointTable(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	origOpen := sqlOpenOracleDB
	defer func() { sqlOpenOracleDB = origOpen }()
	sqlOpenOracleDB = func(user string, password string, host string, port int, serviceName, connectString string) (*sql.DB, error) {
		return db, nil
	}
	tableNameRow := sqlmock.NewRows([]string{"TABLE_NAME"}).AddRow("checkPoint")
	clusterIDRow := sqlmock.NewRows([]string{"CLUSTERID"}).AddRow("12345")
	checkPointRow := sqlmock.NewRows([]string{"CHECKPOINT"}).
		AddRow(`{"commitTS": 1024, "consistent": true, "ts-map": {"primary-ts": 2000, "secondary-ts": 1999}}`)
	mock.ExpectQuery("select table_name.*").WillReturnRows(tableNameRow)
	mock.ExpectQuery("select clusterID from.*").WillReturnRows(clusterIDRow)
	mock.ExpectQuery("select checkPoint from.*").WillReturnRows(checkPointRow)

	cp, err := newOracle(&Config{
		CheckpointType: "oracle",
		Db: &DBConfig{
			User:              "user-1",
			OracleServiceName: "service-name",
		},
	})
	pcp := cp.(*OracleCheckPoint)
	c.Assert(err, IsNil)
	c.Assert(cp, NotNil)
	c.Assert(pcp.CommitTS, Equals, int64(1024))
	c.Assert(pcp.ConsistentSaved, Equals, true)
	c.Assert(pcp.TsMap["primary-ts"], Equals, int64(2000))
	c.Assert(pcp.TsMap["secondary-ts"], Equals, int64(1999))
	c.Assert(pcp.table, Equals, "tidb_binlog_checkpoint")
	c.Assert(pcp.schema, Equals, "user-1")
}

func (s *newOracleSuite) TestConfigCheckPointTable(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	origOpen := sqlOpenOracleDB
	defer func() { sqlOpenOracleDB = origOpen }()
	sqlOpenOracleDB = func(user string, password string, host string, port int, serviceName, connectString string) (*sql.DB, error) {
		return db, nil
	}
	tableNameRow := sqlmock.NewRows([]string{"TABLE_NAME"}).AddRow("checkPoint")
	clusterIDRow := sqlmock.NewRows([]string{"CLUSTERID"}).AddRow("12345")
	checkPointRow := sqlmock.NewRows([]string{"CHECKPOINT"}).
		AddRow(`{"commitTS": 1024, "consistent": true, "ts-map": {"primary-ts": 2000, "secondary-ts": 1999}}`)
	mock.ExpectQuery("select table_name.*").WillReturnRows(tableNameRow)
	mock.ExpectQuery("select clusterID from.*").WillReturnRows(clusterIDRow)
	mock.ExpectQuery("select checkPoint from.*").WillReturnRows(checkPointRow)

	cp, err := newOracle(&Config{
		CheckpointType: "oracle",
		Db: &DBConfig{
			User:              "user-1",
			OracleServiceName: "service-name",
		},
		Table: "new_table_name",
	})
	pcp := cp.(*OracleCheckPoint)
	c.Assert(err, IsNil)
	c.Assert(cp, NotNil)
	c.Assert(pcp.table, Equals, "new_table_name")
	c.Assert(pcp.schema, Equals, "user-1")
}
