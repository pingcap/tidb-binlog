package syncer

import (
	"github.com/pingcap/check"
)

type testMemorySuite struct{}

var _ = check.Suite(&testMemorySuite{})

func (s *testMemorySuite) TestMemorySyncer(c *check.C) {
	syncer, err := newMemSyncer()
	c.Assert(err, check.IsNil)

	syncTest(c, Syncer(syncer))

	binlog := syncer.GetBinlogs()
	c.Assert(binlog, check.HasLen, 2)

	err = syncer.Close()
	c.Assert(err, check.IsNil)
}
