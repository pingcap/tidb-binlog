package syncer

import (
	"github.com/pingcap/check"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/mysql"
	//"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
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
	col1 := &pb.Column{
		Name:      "a",
		Tp:        []byte{mysql.TypeInt24},
		MysqlType: "int",
		Value:     encodeIntValue(1),
	}
	col2 := &pb.Column{
		Name:      "b",
		Tp:        []byte{mysql.TypeVarchar},
		MysqlType: "varchar",
		Value:     encodeBytesValue([]byte("test")),
	}
	col3 := &pb.Column{
		Name:      "c",
		Tp:        []byte{mysql.TypeVarchar},
		MysqlType: "varchar",
		Value:     encodeBytesValue([]byte("test")),
		ChangedValue: encodeBytesValue([]byte("abc")),
	}

	col1Bytes, err := col1.Marshal()
	c.Assert(err, check.IsNil)
	col1Str := getInsertOrDeleteColumnStr(col1Bytes)
	c.Assert(col1Str, check.Equals, "a(int): 1 \n")

	col2Bytes, err := col2.Marshal()
	c.Assert(err, check.IsNil)
	col2Str := getInsertOrDeleteColumnStr(col2Bytes)
	c.Assert(col2Str, check.Equals, "b(varchar): test \n")

	col3Bytes, err := col3.Marshal()
	c.Assert(err, check.IsNil)
	col3Str := getUpdateColumnStr(col3Bytes)
	c.Assert(col3Str, check.Equals, "c(varchar): test => abc\n")

	
	insertEvent := &pb.Event {
		Tp:  pb.EventType_Insert,
		Row: [][]byte{col1Bytes, col2Bytes}, 
	}
	eventStr := getEventDataStr(insertEvent)
	c.Assert(eventStr, check.Equals, "a(int): 1 \nb(varchar): test \n")

	deleteEvent := &pb.Event {
		Tp:  pb.EventType_Delete,
		Row: [][]byte{col1Bytes, col2Bytes}, 
	}
	eventStr = getEventDataStr(deleteEvent)
	c.Assert(eventStr, check.Equals, "a(int): 1 \nb(varchar): test \n")

	updateEvent := &pb.Event {
		Tp:  pb.EventType_Delete,
		Row: [][]byte{col3Bytes, col3Bytes}, 
	}
	eventStr = getEventDataStr(updateEvent)
	c.Assert(eventStr, check.Equals, "c(varchar): test \nc(varchar): test \n")
}

func encodeIntValue(value int64) []byte {
	b := make([]byte, 0, 5)
	// 3 means intFlag
	b = append(b, 3)
	b = codec.EncodeInt(b, value)
	return b
}

func encodeBytesValue(value []byte) []byte {
	b := make([]byte, 0, 5)
	// 1 means bytesFlag
	b = append(b, 1)
	b = codec.EncodeBytes(b, value)
	return b
}