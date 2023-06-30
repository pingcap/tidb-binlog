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
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb/parser/model"
	ptypes "github.com/pingcap/tidb/parser/types"
	pb "github.com/pingcap/tidb/tidb-binlog/proto/go-binlog"

	. "github.com/pingcap/check"
)

type secondaryBinlogToTxnSuite struct{}

var _ = Suite(&secondaryBinlogToTxnSuite{})

func (s *secondaryBinlogToTxnSuite) TestTranslateDDL(c *C) {
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
	txn, err := SecondaryBinlogToTxn(&binlog, nil, false)
	c.Assert(err, IsNil)
	c.Assert(txn.DDL.Database, Equals, db)
	c.Assert(txn.DDL.Table, Equals, table)
	c.Assert(txn.DDL.SQL, Equals, sql)
}

func (s *secondaryBinlogToTxnSuite) TestTranslateOracleDDL(c *C) {
	db, table := "test", "t1"
	sql := "truncate table test.t1"
	binlog := pb.Binlog{
		Type: pb.BinlogType_DDL,
		DdlData: &pb.DDLData{
			SchemaName: &db,
			TableName:  &table,
			DdlQuery:   []byte(sql),
		},
	}
	rules := []*router.TableRule{
		{SchemaPattern: "test", TablePattern: "t1", TargetSchema: "test_routed", TargetTable: "t1_routed"},
	}
	router, _ := router.NewTableRouter(false, rules)
	txn, err := SecondaryBinlogToTxn(&binlog, router, true)
	c.Assert(err, IsNil)
	c.Assert(txn.DDL.Database, Equals, "test_routed")
	c.Assert(txn.DDL.Table, Equals, "t1_routed")
	c.Assert(txn.DDL.SQL, Equals, sql)
}

func (s *secondaryBinlogToTxnSuite) TestTranslateDML(c *C) {
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
	txn, err := SecondaryBinlogToTxn(&binlog, nil, false)
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

func (s *secondaryBinlogToTxnSuite) TestTranslateOracleDML(c *C) {
	db, table := "test", "t1"
	rules := []*router.TableRule{
		{SchemaPattern: "test", TablePattern: "t1", TargetSchema: "test_routed", TargetTable: "t1_routed"},
	}
	router, _ := router.NewTableRouter(false, rules)
	var c1Old, c1New int64 = 1, 2
	c2Old, c2New := "2021-12-17 15:34:30.123456", "2021-12-17 15:34:31.654321"
	c3Old, c3New := 123456789.123456, 123456789.654321
	c1ColumnInfo := &pb.ColumnInfo{
		Name:         "c1",
		MysqlType:    "bigint",
		IsPrimaryKey: true,
		Flen:         10,
		Decimal:      0,
	}
	c2ColumnInfo := &pb.ColumnInfo{
		Name:         "c2",
		MysqlType:    "datetime",
		IsPrimaryKey: false,
		Flen:         0,
		Decimal:      6,
	}
	c3ColumnInfo := &pb.ColumnInfo{
		Name:         "c3",
		MysqlType:    "double",
		IsPrimaryKey: false,
		Flen:         16,
		Decimal:      5,
	}
	dml := pb.DMLData{
		Tables: []*pb.Table{
			{
				SchemaName: &db,
				TableName:  &table,
				ColumnInfo: []*pb.ColumnInfo{
					c1ColumnInfo,
					c2ColumnInfo,
					c3ColumnInfo,
				},
				Mutations: []*pb.TableMutation{
					{
						Type: pb.MutationType_Update.Enum(),
						Row: &pb.Row{
							Columns: []*pb.Column{
								{Int64Value: &c1New},
								{StringValue: &c2New},
								{DoubleValue: &c3New},
							},
						},
						ChangeRow: &pb.Row{
							Columns: []*pb.Column{
								{Int64Value: &c1Old},
								{StringValue: &c2Old},
								{DoubleValue: &c3Old},
							},
						},
					},
					{
						Type: pb.MutationType_Insert.Enum(),
						Row: &pb.Row{
							Columns: []*pb.Column{
								{Int64Value: &c1New},
								{StringValue: &c2New},
								{DoubleValue: &c3New},
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
	txn, err := SecondaryBinlogToTxn(&binlog, router, true)
	c.Assert(err, IsNil)
	c.Assert(txn.DMLs, HasLen, 2)
	for _, dml := range txn.DMLs {
		c.Assert(dml.Database, Equals, "test_routed")
		c.Assert(dml.Table, Equals, "t1_routed")
	}
	update := txn.DMLs[0]
	c.Assert(update.Tp, Equals, UpdateDMLType)
	c.Assert(update.Values, HasLen, 3)
	c.Assert(update.Values["C1"], Equals, c1New)
	c.Assert(update.Values["C2"], Equals, c2New)
	c.Assert(update.Values["C3"], Equals, c3New)
	c.Assert(update.OldValues, HasLen, 3)
	c.Assert(update.OldValues["C1"], Equals, c1Old)
	c.Assert(update.OldValues["C2"], Equals, c2Old)
	c.Assert(update.OldValues["C3"], Equals, c3Old)
	insert := txn.DMLs[1]
	c.Assert(insert.Values, HasLen, 3)
	c.Assert(insert.Values["C1"], Equals, c1New)
	c.Assert(insert.Values["C2"], Equals, c2New)
	c.Assert(insert.Values["C3"], Equals, c3New)
	checkColumnInfo(c, update.UpColumnsInfoMap["C1"], c1ColumnInfo)
	checkColumnInfo(c, update.UpColumnsInfoMap["C2"], c2ColumnInfo)
	checkColumnInfo(c, update.UpColumnsInfoMap["C2"], c2ColumnInfo)
}

func checkColumnInfo(c *C, txnColInfo *model.ColumnInfo, pbColInfo *pb.ColumnInfo) {
	c.Assert(txnColInfo.Name.O, Equals, pbColInfo.Name)
	c.Assert(txnColInfo.GetType(), Equals, ptypes.StrToType(pbColInfo.MysqlType))
	c.Assert(txnColInfo.GetFlen(), Equals, int(pbColInfo.Flen))
	c.Assert(txnColInfo.GetDecimal(), Equals, int(pbColInfo.Decimal))
}

func (s *secondaryBinlogToTxnSuite) TestGetDMLType(c *C) {
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
