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
	"io/ioutil"
	"path"
	"strings"

	. "github.com/pingcap/check"
)

type metaSuite struct{}

var _ = Suite(&metaSuite{})

func (s *metaSuite) TestMeta(c *C) {
	meta := &Meta{
		CommitTS: 123,
	}
	metaStr := meta.String()
	c.Assert(metaStr, Equals, "commitTS: 123")
}

func (s *metaSuite) TestSaveMeta(c *C) {
	dir := c.MkDir()
	filename := path.Join(dir, "savepoint")
	err := saveMeta(filename, 123, "Local")
	c.Assert(err, IsNil)

	b, err := ioutil.ReadFile(filename)
	c.Assert(err, IsNil)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	c.Assert(lines[0], Equals, "commitTS = 123")
}
