package drainer

import (
	. "github.com/pingcap/check"
	"regexp"
)

func (t *testDrainerSuite) TestFilter(c *C) {
	syncer := new(Syncer)

	syncer.cfg = new(SyncerConfig)
	syncer.cfg.DoDBs = []string{"fulldb", "~fulldb_re.*"}
	syncer.cfg.DoTables = []TableName{{"db", "table"}, {"db2", "~table"}}

	syncer.reMap = make(map[string]*regexp.Regexp)
	syncer.genRegexMap()

	c.Assert(syncer.skipDDL("Fulldb", "t1"), IsFalse)
	c.Assert(syncer.skipDDL("fulldb_re_x", ""), IsFalse)
	c.Assert(syncer.skipDML("db", "table_skip"), IsTrue)
	c.Assert(syncer.skipDML("db2", "table"), IsFalse)
}
