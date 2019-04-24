package syncer

import (
	"github.com/pingcap/check"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

type testPrintSuite struct{}

var _ = check.Suite(&testPrintSuite{})

func (s *testPrintSuite) TestPrintSyncer(c *check.C) {
	syncer, err := newPrintSyncer()
	c.Assert(err, check.IsNil)

	ddlBinlog := &pb.Binlog{
		Tp:       pb.BinlogType_DDL,
		DdlQuery: []byte("create database test;"),
	}
	dmlBinlog := &pb.Binlog{
		Tp: pb.BinlogType_DML,
	}

	err = syncer.Sync(ddlBinlog, func(binlog *pb.Binlog) {})
	c.Assert(err, check.IsNil)

	err = syncer.Sync(dmlBinlog, func(binlog *pb.Binlog) {})
	c.Assert(err, check.IsNil)

	err = syncer.Close()
	c.Assert(err, check.IsNil)
}

func (s *testPrintSuite) TestPrintEventHeader(c *check.C) {
	schema := "test"
	table := "t1"
	event := &pb.Event{
		Tp:         pb.EventType_Insert,
		SchemaName: &schema,
		TableName:  &table,
	}

	eventHeaderStr := getEventHeaderStr(event)
	c.Assert(eventHeaderStr, check.Equals, "schema: test; table: t1; type: Insert\n")
}

func (s *testPrintSuite) TestPrintDDL(c *check.C) {
	ddlBinlog := &pb.Binlog{
		Tp:       pb.BinlogType_DDL,
		DdlQuery: []byte("create database test;"),
	}
	ddlStr := getDDLStr(ddlBinlog)
	c.Assert(ddlStr, check.Equals, "DDL query: create database test;\n")
}

func (s *testPrintSuite) TestPrintRow(c *check.C) {
	cols := generateColumns(c)

	col1Str := getInsertOrDeleteColumnStr(cols[0])
	c.Assert(col1Str, check.Equals, "a(int): 1 \n")

	col2Str := getInsertOrDeleteColumnStr(cols[1])
	c.Assert(col2Str, check.Equals, "b(varchar): test \n")

	col3Str := getUpdateColumnStr(cols[2])
	c.Assert(col3Str, check.Equals, "c(varchar): test => abc\n")

	insertEvent := &pb.Event{
		Tp:  pb.EventType_Insert,
		Row: [][]byte{cols[0], cols[1]},
	}
	eventStr := getEventDataStr(insertEvent)
	c.Assert(eventStr, check.Equals, "a(int): 1 \nb(varchar): test \n")
	rowStr := getInsertOrDeleteRowStr(insertEvent.Row)
	c.Assert(rowStr, check.Equals, "a(int): 1 \nb(varchar): test \n")

	deleteEvent := &pb.Event{
		Tp:  pb.EventType_Delete,
		Row: [][]byte{cols[0], cols[1]},
	}
	eventStr = getEventDataStr(deleteEvent)
	c.Assert(eventStr, check.Equals, "a(int): 1 \nb(varchar): test \n")
	rowStr = getInsertOrDeleteRowStr(deleteEvent.Row)
	c.Assert(rowStr, check.Equals, "a(int): 1 \nb(varchar): test \n")

	updateEvent := &pb.Event{
		Tp:  pb.EventType_Update,
		Row: [][]byte{cols[2]},
	}
	eventStr = getEventDataStr(updateEvent)
	c.Assert(eventStr, check.Equals, "c(varchar): test => abc\n")
	rowStr = getUpdateRowStr(updateEvent.Row)
	c.Assert(rowStr, check.Equals, "c(varchar): test => abc\n")
}
