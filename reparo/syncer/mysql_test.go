package syncer

import (
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/check"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

type testMysqlSuite struct{}

var _ = check.Suite(&testMysqlSuite{})

func (s *testMysqlSuite) TestMysqlSyncer(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	syncer, err := newMysqlSyncerFromSQLDB(db)
	c.Assert(err, check.IsNil)

	ddlBinlog := &pb.Binlog{
		Tp:       pb.BinlogType_DDL,
		DdlQuery: []byte("create database test;"),
	}
	dmlBinlog := &pb.Binlog{
		Tp: pb.BinlogType_DML,
	}

	mock.ExpectBegin()
	mock.ExpectExec("create database test").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	binlogs := make([]*pb.Binlog, 0, 1)
	err = syncer.Sync(ddlBinlog, func(binlog *pb.Binlog) {
		c.Log(binlog)
		binlogs = append(binlogs, binlog)
	})
	c.Assert(err, check.IsNil)
	time.Sleep(100 * time.Millisecond)
	c.Assert(binlogs, check.HasLen, 1)

	err = syncer.Sync(dmlBinlog, func(binlog *pb.Binlog) {})
	c.Assert(err, check.IsNil)

	err = syncer.Close()
	c.Assert(err, check.IsNil)
}
