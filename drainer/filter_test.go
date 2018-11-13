package drainer

import (
	. "github.com/pingcap/check"
)

func (t *testDrainerSuite) TestFilter(c *C) {

	DoDBs := []string{"fulldb", "~fulldb_re.*"}
	DoTables := []TableName{{"db", "table"}, {"db2", "~table"}}

	filter := newFilter(nil, DoDBs, DoTables)

	c.Assert(filter.skipSchemaAndTable("Fulldb", "t1"), IsFalse)
	c.Assert(filter.skipSchemaAndTable("fulldb_re_x", ""), IsFalse)
	c.Assert(filter.skipSchemaAndTable("db", "table_skip"), IsTrue)
	c.Assert(filter.skipSchemaAndTable("db2", "table"), IsFalse)

	// with ignores
	filter = newFilter(map[string]struct{}{"db2": {}}, DoDBs, DoTables)
	c.Assert(filter.skipSchemaAndTable("Fulldb", "t1"), IsFalse)
	c.Assert(filter.skipSchemaAndTable("fulldb_re_x", ""), IsFalse)
	c.Assert(filter.skipSchemaAndTable("db", "table_skip"), IsTrue)
	c.Assert(filter.skipSchemaAndTable("db2", "table"), IsTrue)
}
