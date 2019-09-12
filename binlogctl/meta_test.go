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
	"context"
	"io/ioutil"
	"path"
	"strings"

	. "github.com/pingcap/check"
	pd "github.com/pingcap/pd/client"
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
	c.Assert(lines, HasLen, 3)
	c.Assert(lines[0], Equals, "commitTS = 123")
	c.Assert(lines[1], Equals, "1970-01-01 00:00:00 +0000 UTC")
	// the output depends on the local's timezone
	c.Assert(lines[2], Matches, "1970-01-0.*")
}

type dummyCli struct {
	pd.Client
	physical, logical int64
	err               error
}

func (c dummyCli) GetTS(ctx context.Context) (int64, int64, error) {
	return c.physical, c.logical, c.err
}

func newFakePDClient([]string, pd.SecurityOption) (pd.Client, error) {
	return &dummyCli{
		physical: 123,
		logical:  456,
	}, nil
}

func (s *metaSuite) TestGenerateMetaInfo(c *C) {
	newPDClientFunc = newFakePDClient
	defer func() {
		newPDClientFunc = pd.NewClient
	}()

	dir := c.MkDir()
	cfg := &Config{
		DataDir:  dir,
		EtcdURLs: "127.0.0.1:2379",
	}

	err := GenerateMetaInfo(cfg)
	c.Assert(err, IsNil)

	b, err := ioutil.ReadFile(path.Join(dir, "savepoint"))
	c.Assert(err, IsNil)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	c.Assert(lines, HasLen, 1)
	c.Assert(lines[0], Equals, "commitTS = 32244168")
}
