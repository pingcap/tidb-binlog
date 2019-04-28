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
		DmlData: &pb.DMLData{
			Events: generateDMLEvents(c)[0:1],
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec("create database test").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	mock.ExpectQuery("SELECT column_name, extra FROM information_schema.columns").WithArgs("test", "t1").WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).AddRow("a", "").AddRow("b", ""))

	rows := sqlmock.NewRows([]string{"non_unique", "index_name", "seq_in_index", "column_name"})
	mock.ExpectQuery("SELECT non_unique, index_name, seq_in_index, column_name FROM information_schema.statistics").
		WithArgs("test", "t1").
		WillReturnRows(rows)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO").WithArgs(1, "test").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	binlogs := make([]*pb.Binlog, 0, 2)
	err = syncer.Sync(ddlBinlog, func(binlog *pb.Binlog) {
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

	err = syncer.Close()
	c.Assert(err, check.IsNil)
}
