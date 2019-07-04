package syncer

import (
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
	originWorkerCount := defaultWorkerCount
	defaultWorkerCount = 1
	defer func() {
		defaultWorkerCount = originWorkerCount
	}()

	oldCreateDB := createDB
	createDB = func(string, string, string, int) (db *sql.DB, err error) {
		db, mock, err = sqlmock.New()
		return
	}
	defer func() {
		createDB = oldCreateDB
	}()

	syncer, err := newMysqlSyncer(&DBConfig{}, safemode)
	c.Assert(err, check.IsNil)

	mock.ExpectBegin()
	mock.ExpectExec("create database test").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	mock.ExpectQuery("show columns from `test`.`t1`").WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).AddRow("a", "int", "YES", "", "NULL", "").AddRow("b", "varchar(24)", "YES", "", "NULL", "").AddRow("c", "varchar(24)", "YES", "", "NULL", ""))

	rows := sqlmock.NewRows([]string{"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment"})
	mock.ExpectQuery("show index from `test`.`t1`").WillReturnRows(rows)

	mock.ExpectBegin()
	insertPattern := "INSERT INTO"
	if safemode {
		insertPattern = "REPLACE INTO"
	}
	mock.ExpectExec(insertPattern).WithArgs(1, "test", nil).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM").WithArgs(1, "test").WillReturnResult(sqlmock.NewResult(0, 1))
	if safemode {
		mock.ExpectExec("DELETE FROM").WithArgs().WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(insertPattern).WithArgs(nil, nil, "abc").WillReturnResult(sqlmock.NewResult(0, 1))
	} else {
		mock.ExpectExec("UPDATE").WithArgs("abc", "test").WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectCommit()

	syncTest(c, Syncer(syncer))

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
