package filter

import (
	"testing"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) { TestingT(t) }

type testFilterSuite struct {
}

var _ = Suite(&testFilterSuite{})

func (t *testFilterSuite) TestFilter(c *C) {
	DoDBs := []string{"fulldb", "~fulldb_re.*"}
	DoTables := []TableName{{"db", "table"}, {"db2", "~table"}}

	filter := NewFilter(nil, nil, DoDBs, DoTables)

	c.Assert(filter.SkipSchemaAndTable("Fulldb", "t1"), IsFalse)
	c.Assert(filter.SkipSchemaAndTable("fulldb_re_x", ""), IsFalse)
	c.Assert(filter.SkipSchemaAndTable("db", "table_skip"), IsTrue)
	c.Assert(filter.SkipSchemaAndTable("db2", "table"), IsFalse)

	// with ignore db
	filter = NewFilter([]string{"db2"}, nil, DoDBs, DoTables)
	c.Assert(filter.SkipSchemaAndTable("Fulldb", "t1"), IsFalse)
	c.Assert(filter.SkipSchemaAndTable("fulldb_re_x", ""), IsFalse)
	c.Assert(filter.SkipSchemaAndTable("db", "table_skip"), IsTrue)
	c.Assert(filter.SkipSchemaAndTable("db2", "table"), IsTrue)

	// with ignore table
	ignoreTables := []TableName{{"ignore", "ignore"}}
	filter = NewFilter(nil, ignoreTables, nil, nil)
	c.Assert(filter.SkipSchemaAndTable("ignore", "ignore"), IsTrue)
	c.Assert(filter.SkipSchemaAndTable("not_ignore", "not_ignore"), IsFalse)
}
