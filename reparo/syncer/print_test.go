package syncer

import (
	"github.com/pingcap/check"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

type testPrintSuite struct{}

var _ = check.Suite(&testPrintSuite{})

func (s *testPrintSuite) TestPrintSyncerStr(c *check.C) {
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

	ddlStr := getDDLStr(ddlBinlog)
	c.Assert(ddlStr, check.Equals, "DDL query: create database test;\n")

	schema := "test"
	table := "t1"
	event := &pb.Event{
		Tp:         pb.EventType_Insert,
		SchemaName: &schema,
		TableName:  &table,
	}

	eventHeaderStr := getEventHeaderStr(event)
	c.Assert(eventHeaderStr, check.Equals, "schema: test; table: t1; type: Insert\n")

	/*
	userIDCol := &model.ColumnInfo{
		ID:     1,
		Name:   model.NewCIStr("ID"),
		Offset: 0,
		FieldType: types.FieldType{
			Tp:      mysql.TypeLong,
			Flag:    mysql.BinaryFlag,
			Flen:    11,
			Decimal: -1,
			Charset: "binary",
			Collate: "binary",
		},
		State: model.StatePublic,
	}
	*/

	/*
	str := new(types.Datum)
	*str = types.NewStringDatum("a")
	var raw []byte
	raw = append(raw, bytesFlag)
	raw = EncodeBytes(raw, str.GetBytes())
	*/

	str := new(types.Datum)
	*str = types.NewStringDatum("a")
	c.Log(str.GetBytes())
	_, val, err := codec.DecodeOne(encodeIntValue(1))
	c.Log(val)
	c.Assert(err, check.IsNil)
	//c.Assert(err, check.NotNil)

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

	colBytes, err := col1.Marshal()
	c.Assert(err, check.IsNil)
	colStr := getInsertOrDeleteColStr(colBytes)
	c.Assert(colStr, check.Equals, "a(int): 1 \n")

	colBytes, err = col2.Marshal()
	c.Assert(err, check.IsNil)
	colStr = getInsertOrDeleteColStr(colBytes)
	c.Assert(colStr, check.Equals, "b(varchar): test \n")
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