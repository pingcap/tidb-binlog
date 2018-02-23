package drainer

import (
	. "github.com/pingcap/check"
)

func (t *testDrainerSuite) TestFilter(c *C) {
	syncer := new(Syncer)

	syncer.cfg = new(SyncerConfig)
	syncer.cfg.DoDBs = []string{"fulldb", "~fulldb_re.*"}
	syncer.cfg.DoTables = []TableName{{"db", "table"}, {"db2", "~table"}}
	syncer.filter = newFilter(syncer.cfg.DoDBs, syncer.cfg.DoTables, "")

	c.Assert(syncer.filter.SkipSchemaAndTable("Fulldb", "t1"), IsFalse)
	c.Assert(syncer.filter.SkipSchemaAndTable("fulldb_re_x", ""), IsFalse)
	c.Assert(syncer.filter.SkipSchemaAndTable("db", "table_skip"), IsTrue)
	c.Assert(syncer.filter.SkipSchemaAndTable("db2", "table"), IsFalse)
}
