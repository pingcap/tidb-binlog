// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the
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
	. "github.com/pingcap/check"
)

type kafkaSuite struct{}

var _ = Suite(&kafkaSuite{})

func (s *kafkaSuite) TestNewSaramaConfig(c *C) {
	cfg, err := NewSaramaConfig("0.8.2.0", "testing")
	c.Assert(err, IsNil)
	c.Assert(cfg, NotNil)
	c.Assert(cfg.Version.String(), Equals, "0.8.2.0")
	c.Assert(cfg.ClientID, Equals, "tidb_binlog")
}
