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

package translator

import (
	"fmt"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

type testMysqlSuite struct {
	BinlogGenrator
}

var _ = check.Suite(&testMysqlSuite{})

func (t *testMysqlSuite) TestGenColumnList(c *check.C) {
	table := testGenTable("normal")
	c.Assert(genColumnNameList(table.Columns), check.DeepEquals, []string{"ID", "NAME", "SEX"})
}

func (t *testMysqlSuite) TestDDL(c *check.C) {
	t.SetDDL()

	txn, err := TiBinlogToTxn(t, t.Schema, t.Table, t.TiBinlog, nil)
	c.Assert(err, check.IsNil)

	c.Assert(txn, check.DeepEquals, &loader.Txn{
		DDL: &loader.DDL{
			Database: t.Schema,
			Table:    t.Table,
			SQL:      string(t.TiBinlog.GetDdlQuery()),
		},
	})
}

func (t *testMysqlSuite) testDML(c *check.C, tp loader.DMLType) {
	txn, err := TiBinlogToTxn(t, t.Schema, t.Table, t.TiBinlog, t.PV)
	c.Assert(err, check.IsNil)

	c.Assert(txn.DMLs, check.HasLen, 1)
	c.Assert(txn.DDL, check.IsNil)

	dml := txn.DMLs[0]
	c.Assert(dml.Tp, check.Equals, tp)

	tableID := t.PV.Mutations[0].TableId
	info, _ := t.TableByID(tableID)
	schema, table, _ := t.SchemaAndTableName(tableID)

	c.Assert(dml.Database, check.Equals, schema)
	c.Assert(dml.Table, check.Equals, table)

	var oldDatums []types.Datum
	if tp == loader.UpdateDMLType {
		oldDatums = t.getOldDatums()
	}
	checkMysqlColumns(c, info, dml, t.getDatums(), oldDatums)
}

func (t *testMysqlSuite) TestInsert(c *check.C) {
	t.SetInsert(c)
	t.testDML(c, loader.InsertDMLType)
}

func (t *testMysqlSuite) TestUpdate(c *check.C) {
	t.SetUpdate(c)
	t.testDML(c, loader.UpdateDMLType)
}

func (t *testMysqlSuite) TestDelete(c *check.C) {
	t.SetDelete(c)
	t.testDML(c, loader.DeleteDMLType)
}

func checkMysqlColumns(c *check.C, info *model.TableInfo, dml *loader.DML, datums []types.Datum, oldDatums []types.Datum) {
	for i, column := range info.Columns {
		myValue := dml.Values[column.Name.O]
		checkMysqlColumn(c, column, myValue, datums[i])

		if oldDatums != nil {
			myValue := dml.OldValues[column.Name.O]
			checkMysqlColumn(c, column, myValue, oldDatums[i])
		}
	}
}

func checkMysqlColumn(c *check.C, col *model.ColumnInfo, myValue interface{}, datum types.Datum) {
	tiStr, err := datum.ToString()
	c.Assert(err, check.IsNil)

	if col.Tp == mysql.TypeEnum {
		tiStr = fmt.Sprintf("%d", datum.GetInt64())
	}

	// tidb encode string type datums as bytes
	// so we get bytes type datums for txn
	if slice, ok := myValue.([]byte); ok {
		myValue = string(slice)
	}

	myStr := fmt.Sprintf("%v", myValue)
	c.Assert(myStr, check.Equals, tiStr)
}
