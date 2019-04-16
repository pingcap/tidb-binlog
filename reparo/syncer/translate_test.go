package syncer

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/codec"
)

func Test(t *testing.T) { check.TestingT(t) }

type testTranslateSuite struct{}

var _ = check.Suite(&testTranslateSuite{})

func (s *testTranslateSuite) TestPBBinlogToTxn(c *check.C) {
	tests := map[*pb.Binlog]*loader.Txn{
		&pb.Binlog{
			Tp:       pb.BinlogType_DDL,
			DdlQuery: []byte("use db1; create table table1(id int)"),
		}: &loader.Txn{
			DDL: &loader.DDL{
				SQL: "use db1; create table table1(id int)",
			},
		},
		&pb.Binlog{
			Tp: pb.BinlogType_DML,
			DmlData: &pb.DMLData{
				Events: generateDMLEvents(),
			},
		}: &loader.Txn{
			DMLs: []*loader.DML{
				{
					Database: "test",
					Table:    "t1",
					Tp:       loader.InsertDMLType,
					Values:   map[string]interface{}{
						"a": int64(1),
						"b": "test",
					},
				}, {
					Database: "test",
					Table:    "t1",
					Tp:       loader.DeleteDMLType,
					Values:   map[string]interface{}{
						"a": int64(1),
						"b": "test",
					},
				}, {
					Database: "test",
					Table:    "t1",
					Tp:       loader.UpdateDMLType,
					Values:   map[string]interface{}{
						"c": "abc",
					},
					OldValues:   map[string]interface{}{
						"c": "test",
					},
				},
			},
		},
	}

	for binlog, txn := range tests {
		getTxn, err := pbBinlogToTxn(binlog)
		c.Assert(err, check.IsNil)
		c.Assert(getTxn.DDL, check.DeepEquals, txn.DDL)
		for i, dml := range getTxn.DMLs {
			c.Assert(dml.Database, check.Equals, txn.DMLs[i].Database)
			c.Assert(dml.Table, check.Equals, txn.DMLs[i].Table)
			c.Assert(dml.Tp, check.Equals, txn.DMLs[i].Tp)
			c.Assert(dml.Values, check.DeepEquals, txn.DMLs[i].Values)
			c.Assert(dml.OldValues, check.DeepEquals, txn.DMLs[i].OldValues)
		}
	}
}

func (s *testTranslateSuite) TestGenColsAndArgs(c *check.C) {
	cols, args, err := genColsAndArgs(generateColumns())
	c.Assert(err, check.IsNil)
	c.Assert(cols, check.DeepEquals, []string{"a", "b", "c"})
	c.Assert(args, check.DeepEquals, []interface {}{int64(1), "test", "test"})
}

// generateDMLEvents generates three DML Events for test.
func generateDMLEvents() []pb.Event {
	schema := "test"
	table := "t1"
	cols := generateColumns()

	return []pb.Event {
		{
			Tp: pb.EventType_Insert,
			SchemaName: &schema,
			TableName:  &table, 
			Row: [][]byte{cols[0], cols[1]},
		},{
			Tp: pb.EventType_Delete,
			SchemaName: &schema,
			TableName:  &table, 
			Row: [][]byte{cols[0], cols[1]},
		},{
			Tp: pb.EventType_Update,
			SchemaName: &schema,
			TableName:  &table, 
			Row: [][]byte{cols[2]},
		},
	}
}

// generateColumns generates three columns for test, the last one used for update.
func generateColumns() [][]byte {
	colsBytes := make([][]byte, 0, 3)
	
	cols := []*pb.Column {
		{
			Name:      "a",
			Tp:        []byte{mysql.TypeInt24},
			MysqlType: "int",
			Value:     encodeIntValue(1),
		}, {
			Name:      "b",
			Tp:        []byte{mysql.TypeVarchar},
			MysqlType: "varchar",
			Value:     encodeBytesValue([]byte("test")),
		}, {
			Name:      "c",
			Tp:        []byte{mysql.TypeVarchar},
			MysqlType: "varchar",
			Value:     encodeBytesValue([]byte("test")),
			ChangedValue: encodeBytesValue([]byte("abc")),
		},
	}

	for _, col := range cols {
		colBytes, err := col.Marshal()
		if err != nil {
			panic(err)
		}

		colsBytes = append(colsBytes, colBytes)
	}

	return colsBytes
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