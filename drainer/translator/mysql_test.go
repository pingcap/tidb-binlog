package translator

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/types"
)

// test the already implemented translator, register and unregister function
func (t *testTranslatorSuite) TestGenColumnList(c *C) {
	m := testGenMysqlTranslator(c)
	table := testGenTable("normal")
	c.Assert(m.genColumnList(table.Columns), Equals, "`ID`,`NAME`,`SEX`")
}

func (t *testTranslatorSuite) TestGenColumnPlaceholders(c *C) {
	m := testGenMysqlTranslator(c)
	c.Assert(m.genColumnPlaceholders(3), Equals, "?,?,?")
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
	cols, err := m.pkIndexColumns(table)
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 2)

	table = testGenTable("hasID")
	cols, err = m.pkIndexColumns(table)
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 1)

	table = testGenTable("normal")
	cols, err = m.pkIndexColumns(table)
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 0)
}

func (t *testTranslatorSuite) testGenerateColumnAndValue(c *C) {
	m := testGenMysqlTranslator(c)
	table := testGenTable("normal")
	rawData, expected := testGenRowData(c, table.Columns, 1)
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
