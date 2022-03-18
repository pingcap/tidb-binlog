package syncer

import (
	"crypto/tls"
	"database/sql"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/check"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

type testMysqlSuite struct{}

var _ = check.Suite(&testMysqlSuite{})

func (s *testMysqlSuite) TestMysqlSyncer(c *check.C) {
	s.testMysqlSyncer(c, true)
	s.testMysqlSyncer(c, false)
}

func (s *testMysqlSuite) testMysqlSyncer(c *check.C, safemode bool) {
	var (
		mock sqlmock.Sqlmock
	)

	oldCreateDB := createDB
	createDB = func(string, string, string, int, *tls.Config) (db *sql.DB, err error) {
		db, mock, err = sqlmock.New()
		return
	}
	defer func() {
		createDB = oldCreateDB
	}()

	syncer, err := newMysqlSyncer(&DBConfig{}, 1, 20, safemode)
	c.Assert(err, check.IsNil)

	mock.ExpectBegin()
	mock.ExpectExec("create database test").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	mock.ExpectQuery("SELECT column_name, extra FROM information_schema.columns").WithArgs("test", "t1").WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).AddRow("a", "").AddRow("b", "").AddRow("c", ""))

	rows := sqlmock.NewRows([]string{"non_unique", "index_name", "seq_in_index", "column_name"})
	mock.ExpectQuery("SELECT non_unique, index_name, seq_in_index, column_name FROM information_schema.statistics").
		WithArgs("test", "t1").
		WillReturnRows(rows)

	mock.ExpectBegin()
	insertPattern := "INSERT INTO"
	if safemode {
		insertPattern = "REPLACE INTO"
	}
	mock.ExpectExec(insertPattern).WithArgs(1, "test", "test").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM").WithArgs(1, "test", "test").WillReturnResult(sqlmock.NewResult(0, 1))
	if safemode {
		mock.ExpectExec("DELETE FROM").WithArgs().WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(insertPattern).WithArgs(1, "test", "abc").WillReturnResult(sqlmock.NewResult(0, 1))
	} else {
		mock.ExpectExec("UPDATE").WithArgs(1, "test", "abc", 1, "test", "test").WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectCommit()

	syncTest(c, syncer)

	err = syncer.Close()
	c.Assert(err, check.IsNil)
}

func syncTest(c *check.C, syncer Syncer) {
	ddlBinlog := &pb.Binlog{
		Tp:       pb.BinlogType_DDL,
		DdlQuery: []byte("create database test;"),
	}
	dmlBinlog := &pb.Binlog{
		Tp: pb.BinlogType_DML,
		DmlData: &pb.DMLData{
			Events: generateDMLEvents(c),
		},
	}

	binlogs := make([]*pb.Binlog, 0, 2)
	err := syncer.Sync(ddlBinlog, func(binlog *pb.Binlog) {
		c.Log(binlog)
		binlogs = append(binlogs, binlog)
	})
	c.Assert(err, check.IsNil)

	err = syncer.Sync(dmlBinlog, func(binlog *pb.Binlog) {
		c.Log(binlog)
		binlogs = append(binlogs, binlog)
	})
	c.Assert(err, check.IsNil)

	time.Sleep(100 * time.Millisecond)
	c.Assert(binlogs, check.HasLen, 2)
}
