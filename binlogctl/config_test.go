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

package binlogctl

import (

	. "github.com/pingcap/check"
)

type configSuite struct{}

var _ = Suite(&configSuite{})

func (s *configSuite) TestConfig(c *C) {
	config := NewConfig()
	args := []string{"-pd-urls=127.0.0.1"}
	err := config.Parse(args)
	c.Assert(err, ErrorMatches, ".*parse EtcdURLs error.*")
	
	args = []string{"-cmd=pumps", "-node-id=nodeID", "-pd-urls=127.0.0.1:2379"}
	err = config.Parse(args)
	c.Assert(err, IsNil)
	c.Assert(config.Command, Equals, QueryPumps)
	c.Assert(config.NodeID, Equals, "nodeID")
	c.Assert(config.EtcdURLs, Equals, "127.0.0.1:2379")
}