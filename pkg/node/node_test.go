package node

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testNodeSuite{})

type testNodeSuite struct{}

func (s *testNodeSuite) TestClone(c *C) {
	status := NewStatus("nodeID", "localhost", Online, 100, 407775642342881, 407775645599649)
	status2 := CloneStatus(status)
	c.Assert(status, Not(Equals), status2)
	c.Assert(*status, Equals, *status2)
}