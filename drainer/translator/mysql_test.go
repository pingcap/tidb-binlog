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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/types"
)

func (t *testTranslatorSuite) TestGenColumnList(c *C) {
	m := testGenMysqlTranslator(c)
	table := testGenTable("normal")
	c.Assert(m.genColumnList(table.Columns), Equals, "`ID`,`NAME`,`SEX`")
}

func (t *testTranslatorSuite) TestGenKVs(c *C) {
	m := testGenMysqlTranslator(c)
	table := testGenTable("normal")
	c.Assert(m.genKVs(table.Columns), Equals, "`ID` = ?, `NAME` = ?, `SEX` = ?")
}

func (t *testTranslatorSuite) TestGenWhere(c *C) {
	m := testGenMysqlTranslator(c)
	table := testGenTable("normal")
	where, values, err := m.genWhere(table, table.Columns, []interface{}{1, "test", nil})
	c.Assert(err, IsNil)
	c.Assert(where, Equals, "`ID` = ? and `NAME` = ? and `SEX` is NULL")
	c.Assert(values, DeepEquals, []interface{}{1, "test"})
}

func (t *testTranslatorSuite) TestPkHandleColumn(c *C) {
	m := testGenMysqlTranslator(c)
	table := testGenTable("normal")
	c.Assert(m.pkHandleColumn(table), IsNil)
	table = testGenTable("hasID")
	col := m.pkHandleColumn(table)
	if col == nil {
		c.Fatal("table should has ID")
	}
}

func (t *testTranslatorSuite) TestPkIndexColumns(c *C) {
	m := testGenMysqlTranslator(c)
	table := testGenTable("hasPK")
	cols, err := m.uniqueIndexColumns(table, nil)
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 2)

	table = testGenTable("hasID")
	cols, err = m.uniqueIndexColumns(table, nil)
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 1)

	table = testGenTable("normal")
	cols, err = m.uniqueIndexColumns(table, nil)
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 0)
}

// TODO: useless code fix test?
// nolint
func (t *testTranslatorSuite) testGenerateColumnAndValue(c *C) {
	m := testGenMysqlTranslator(c)
	table := testGenTable("normal")
	rawData, expected, _ := testGenRowData(c, table.Columns, 1)
	rawData = append(rawData, rawData[0])
	data := make(map[int64]types.Datum)
	for index, d := range rawData {
		data[int64(index)] = d
	}

	cols, vals, err := m.generateColumnAndValue(table.Columns, data)
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 3)
	for index := range vals {
		c.Assert(vals[index], DeepEquals, expected[index])
	}
}

func testGenMysqlTranslator(c *C) *mysqlTranslator {
	translator, err := New("mysql")
	c.Assert(err, IsNil)
	m, ok := translator.(*mysqlTranslator)
	c.Assert(ok, IsTrue)
	return m
}
