// Copyright 2019 PingCAP, Inc.
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

package loader

import (
	pb "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"

	. "github.com/pingcap/check"
)

type slaveBinlogToTxnSuite struct{}

var _ = Suite(&slaveBinlogToTxnSuite{})

func (s *slaveBinlogToTxnSuite) TestTranslateDDL(c *C) {
	db, table := "test", "hello"
	sql := "CREATE TABLE hello (id INT AUTO_INCREMENT) PRIMARY KEY(id);"
	binlog := pb.Binlog{
		Type: pb.BinlogType_DDL,
		DdlData: &pb.DDLData{
			SchemaName: &db,
			TableName:  &table,
			DdlQuery:   []byte(sql),
		},
	}
	txn, err := SlaveBinlogToTxn(&binlog)
	c.Assert(err, IsNil)
	c.Assert(txn.DDL.Database, Equals, db)
	c.Assert(txn.DDL.Table, Equals, table)
	c.Assert(txn.DDL.SQL, Equals, sql)
}

func (s *slaveBinlogToTxnSuite) TestTranslateDML(c *C) {
	db, table := "test", "hello"
	var oldVal, newVal int64 = 41, 42
	dml := pb.DMLData{
		Tables: []*pb.Table{
			{
				SchemaName: &db,
				TableName:  &table,
				ColumnInfo: []*pb.ColumnInfo{
					{Name: "uid"},
				},
				Mutations: []*pb.TableMutation{
					{
						Type: pb.MutationType_Update.Enum(),
						Row: &pb.Row{
							Columns: []*pb.Column{
								{Int64Value: &newVal},
							},
						},
						ChangeRow: &pb.Row{
							Columns: []*pb.Column{
								{Int64Value: &oldVal},
							},
						},
					},
					{
						Type: pb.MutationType_Insert.Enum(),
						Row: &pb.Row{
							Columns: []*pb.Column{
								{Int64Value: &newVal},
							},
						},
					},
				},
			},
		},
	}
	binlog := pb.Binlog{
		DmlData: &dml,
	}
	txn, err := SlaveBinlogToTxn(&binlog)
	c.Assert(err, IsNil)
	c.Assert(txn.DMLs, HasLen, 2)
	for _, dml := range txn.DMLs {
		c.Assert(dml.Database, Equals, db)
		c.Assert(dml.Table, Equals, table)
	}
	update := txn.DMLs[0]
	c.Assert(update.Tp, Equals, UpdateDMLType)
	c.Assert(update.Values, HasLen, 1)
	c.Assert(update.Values["uid"], Equals, newVal)
	c.Assert(update.OldValues, HasLen, 1)
	c.Assert(update.OldValues["uid"], Equals, oldVal)
	insert := txn.DMLs[1]
	c.Assert(insert.Values, HasLen, 1)
	c.Assert(insert.Values["uid"], Equals, newVal)
}

func (s *slaveBinlogToTxnSuite) TestGetDMLType(c *C) {
	mut := pb.TableMutation{}
	mut.Type = pb.MutationType(404).Enum()
	c.Assert(getDMLType(&mut), Equals, UnknownDMLType)
	mut.Type = pb.MutationType_Insert.Enum()
	c.Assert(getDMLType(&mut), Equals, InsertDMLType)
	mut.Type = pb.MutationType_Update.Enum()
	c.Assert(getDMLType(&mut), Equals, UpdateDMLType)
	mut.Type = pb.MutationType_Delete.Enum()
	c.Assert(getDMLType(&mut), Equals, DeleteDMLType)
}

type columnToArgSuite struct{}

var _ = Suite(&columnToArgSuite{})

func (s *columnToArgSuite) TestHandleMySQLJSON(c *C) {
	colVal := `{"key": "value"}`
	arg, err := columnToArg("json", &pb.Column{BytesValue: []byte(colVal)})
	c.Assert(err, IsNil)
	c.Assert(arg, Equals, colVal)
}

func (s *columnToArgSuite) TestGetCorrectArgs(c *C) {
	isNull := true
	col := &pb.Column{IsNull: &isNull}
	val, err := columnToArg("", col)
	c.Assert(err, IsNil)
	c.Assert(val, IsNil)

	var i64 int64 = 666
	col = &pb.Column{Int64Value: &i64}
	val, err = columnToArg("", col)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, i64)

	var u64 uint64 = 777
	col = &pb.Column{Uint64Value: &u64}
	val, err = columnToArg("", col)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, u64)

	var d float64 = 3.14
	col = &pb.Column{DoubleValue: &d}
	val, err = columnToArg("", col)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, d)

	var b []byte = []byte{1, 2, 3}
	col = &pb.Column{BytesValue: b}
	val, err = columnToArg("", col)
	c.Assert(err, IsNil)
	c.Assert(val, DeepEquals, b)

	var ss string = "hello world"
	col = &pb.Column{StringValue: &ss}
	val, err = columnToArg("", col)
	c.Assert(err, IsNil)
	c.Assert(val, DeepEquals, ss)
}
