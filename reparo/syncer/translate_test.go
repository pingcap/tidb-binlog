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

package syncer

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/codec"
)

func Test(t *testing.T) { check.TestingT(t) }

type testTranslateSuite struct{}

var _ = check.Suite(&testTranslateSuite{})

func (s *testTranslateSuite) TestPBBinlogToTxn(c *check.C) {
	tests := map[*pb.Binlog]*loader.Txn{
		{
			Tp:       pb.BinlogType_DDL,
			DdlQuery: []byte("use db1;create table table1(id int)"),
		}: {
			DDL: &loader.DDL{
				SQL:      "create table table1(id int)",
				Database: "db1",
				Table:    "table1",
			},
		},
		{
			Tp:       pb.BinlogType_DDL,
			DdlQuery: []byte("create table `db1`.`table1`(id int)"),
		}: {
			DDL: &loader.DDL{
				SQL:      "create table `db1`.`table1`(id int)",
				Database: "db1",
				Table:    "table1",
			},
		},
		{
			Tp:       pb.BinlogType_DDL,
			DdlQuery: []byte("drop database db1"),
		}: {
			DDL: &loader.DDL{
				SQL:      "drop database db1",
				Database: "db1",
			},
		},
		{
			Tp:       pb.BinlogType_DDL,
			DdlQuery: []byte("truncate table `db1`.`table1`"),
		}: {
			DDL: &loader.DDL{
				SQL:      "truncate table `db1`.`table1`",
				Database: "db1",
				Table:    "table1",
			},
		},
		{
			Tp:       pb.BinlogType_DDL,
			DdlQuery: []byte("create index uk on `db1`.`table1` (`uk`)"),
		}: {
			DDL: &loader.DDL{
				SQL:      "create index uk on `db1`.`table1` (`uk`)",
				Database: "db1",
				Table:    "table1",
			},
		},
		{
			Tp:       pb.BinlogType_DDL,
			DdlQuery: []byte("drop index uk on `db1`.`table1`"),
		}: {
			DDL: &loader.DDL{
				SQL:      "drop index uk on `db1`.`table1`",
				Database: "db1",
				Table:    "table1",
			},
		},
		{
			Tp:       pb.BinlogType_DDL,
			DdlQuery: []byte("alter table `db1`.`table1` add column c int"),
		}: {
			DDL: &loader.DDL{
				SQL:      "alter table `db1`.`table1` add column c int",
				Database: "db1",
				Table:    "table1",
			},
		},
		{
			Tp:       pb.BinlogType_DDL,
			DdlQuery: []byte("drop table `db1`.`table1`"),
		}: {
			DDL: &loader.DDL{
				SQL:      "drop table `db1`.`table1`",
				Database: "db1",
				Table:    "table1",
			},
		},
		{
			Tp:       pb.BinlogType_DDL,
			DdlQuery: []byte("rename table `db1`.`table1` to `db1`.`table2`"),
		}: {
			DDL: &loader.DDL{
				SQL:      "rename table `db1`.`table1` to `db1`.`table2`",
				Database: "db1",
				Table:    "table2",
			},
		},
		{
			Tp: pb.BinlogType_DML,
			DmlData: &pb.DMLData{
				Events: generateDMLEvents(c),
			},
		}: {
			DMLs: []*loader.DML{
				{
					Database: "test",
					Table:    "t1",
					Tp:       loader.InsertDMLType,
					Values: map[string]interface{}{
						"a": int64(1),
						"b": "test",
						"c": "test",
					},
				}, {
					Database: "test",
					Table:    "t1",
					Tp:       loader.DeleteDMLType,
					Values: map[string]interface{}{
						"a": int64(1),
						"b": "test",
						"c": "test",
					},
				}, {
					Database: "test",
					Table:    "t1",
					Tp:       loader.UpdateDMLType,
					Values: map[string]interface{}{
						"a": int64(1),
						"b": "test",
						"c": "abc",
					},
					OldValues: map[string]interface{}{
						"a": int64(1),
						"b": "test",
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
		c.Assert(getTxn.DMLs, check.DeepEquals, txn.DMLs)
	}
}

func (s *testTranslateSuite) TestTrimUse(c *check.C) {
	tests := []struct {
		Origin string
		Expect string
	}{
		{"use a;create table a(id int);", "create table a(id int);"},
		{"create database a;", "create database a;"},
	}

	for _, test := range tests {
		get := trimUse(test.Origin)
		c.Assert(get, check.Equals, test.Expect)
	}
}

func (s *testTranslateSuite) TestGenColsAndArgs(c *check.C) {
	cols, args, err := genColsAndArgs(generateColumns(c))
	c.Assert(err, check.IsNil)
	c.Assert(cols, check.DeepEquals, []string{"a", "b", "c"})
	c.Assert(args, check.DeepEquals, []interface{}{int64(1), "test", "test"})
}

// generateDMLEvents generates three DML Events for test.
func generateDMLEvents(c *check.C) []pb.Event {
	schema := "test"
	table := "t1"
	cols := generateColumns(c)

	return []pb.Event{
		{
			Tp:         pb.EventType_Insert,
			SchemaName: &schema,
			TableName:  &table,
			Row:        [][]byte{cols[0], cols[1], cols[2]},
		}, {
			Tp:         pb.EventType_Delete,
			SchemaName: &schema,
			TableName:  &table,
			Row:        [][]byte{cols[0], cols[1], cols[2]},
		}, {
			Tp:         pb.EventType_Update,
			SchemaName: &schema,
			TableName:  &table,
			Row:        [][]byte{cols[0], cols[1], cols[2]},
		},
	}
}

// generateColumns generates three columns for test, the last one used for update.
func generateColumns(c *check.C) [][]byte {
	allColBytes := make([][]byte, 0, 3)

	cols := []*pb.Column{
		{
			Name:         "a",
			Tp:           []byte{mysql.TypeInt24},
			MysqlType:    "int",
			Value:        encodeIntValue(1),
			ChangedValue: encodeIntValue(1),
		}, {
			Name:         "b",
			Tp:           []byte{mysql.TypeVarchar},
			MysqlType:    "varchar",
			Value:        encodeBytesValue([]byte("test")),
			ChangedValue: encodeBytesValue([]byte("test")),
		}, {
			Name:         "c",
			Tp:           []byte{mysql.TypeVarchar},
			MysqlType:    "varchar",
			Value:        encodeBytesValue([]byte("test")),
			ChangedValue: encodeBytesValue([]byte("abc")),
		},
	}

	for _, col := range cols {
		colBytes, err := col.Marshal()
		if err != nil {
			c.Fatal(err)
		}

		allColBytes = append(allColBytes, colBytes)
	}

	return allColBytes
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
