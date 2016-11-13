package translator

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/types"
)

// test the already implemented translater, register and unregister function
func (t *testTranslaterSuite) TestGenColumnList(c *C) {
	m := testGenMysqlTranslator(c)
	table := testGenTable()
	c.Assert(m.genColumnList(table.Columns), Equals, "id,name,sex")
}

func (t *testTranslaterSuite) TestGenColumnPlaceholders(c *C) {
	m := testGenMysqlTranslator(c)
	c.Assert(m.genColumnPlaceholders(3), Equals, "?,?,?")
}

func (t *testTranslaterSuite) TestGenKVs(c *C) {
	m := testGenMysqlTranslator(c)
	table := testGenTable()
	c.Assert(m.genKVs(table.Columns), Equals, "ID = ?, NAME = ?, SEX = ?")
}

func (t *testTranslaterSuite) TestGenWhere(c *C) {
	m := testGenMysqlTranslator(c)
	table := testGenTable()
	c.Assert(m.genWhere(table.Columns, []interface{}{1, "test", nil}), Equals, "ID = ? and NAME = ? and SEX is ?")
}

func (t *testTranslaterSuite) TestPkHandleColumn(c *C) {
	m := testGenMysqlTranslator(c)
	table := testGenTable()
	c.Assert(m.pkHandleColumn(table), IsNil)
	table = testGenTableWithID()
	col := m.pkHandleColumn(table)
	if col == nil {
		c.Fatal("table should has ID")
	}
}

func (t *testTranslaterSuite) TestPkIndexColumns(c *C) {
	m := testGenMysqlTranslator(c)
	table := testGenTableWithPK()
	cols, err := m.pkIndexColumns(table)
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 2)

	table = testGenTableWithID()
	cols, err = m.pkIndexColumns(table)
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 1)

	table = testGenTable()
	cols, err = m.pkIndexColumns(table)
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 0)
}

func (t *testTranslaterSuite) testGenerateColumnAndValue(c *C) {
	m := testGenMysqlTranslator(c)
	table := testGenTable()
	rawData, expected := testGenRowDatas(c, table.Columns)
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
