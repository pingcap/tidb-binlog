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

func (s *testNodeSuite) TestString(c *C) {
	status := NewStatus("nodeID", "localhost", Online, 100, 407775642342881, 407775645599649)
	str := status.String()
	c.Assert(str, Matches, "{NodeID: nodeID, Addr: localhost, State: online, MaxCommitTS: 407775642342881, UpdateTime: 1970-01-19 .*}")
}
