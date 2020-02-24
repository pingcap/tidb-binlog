// Copyright 2020 PingCAP, Inc.
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

package util

import (
	"github.com/pingcap/check"
)

type netSuite struct{}

var _ = check.Suite(&netSuite{})

func (n *netSuite) TestListen(c *check.C) {
	// wrong addr
	_, err := Listen("unix", "://asdf:1231:123:12", nil)
	c.Assert(err, check.ErrorMatches, ".*invalid .* socket addr.*")

	// unbindable addr
	_, err = Listen("tcp", "http://asdf;klj:7979/12", nil)
	c.Assert(err, check.ErrorMatches, ".*fail to start.*")

	// return listener
	l, err := Listen("tcp", "http://localhost:17979", nil)
	c.Assert(err, check.IsNil)
	c.Assert(l, check.NotNil)
}
