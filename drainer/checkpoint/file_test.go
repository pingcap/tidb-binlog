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

package checkpoint

import (
	"os"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

func (t *testCheckPointSuite) TestFile(c *C) {
	fileName := "/tmp/test"
	notExistFileName := "test_not_exist"
	cfg := new(Config)
	cfg.CheckPointFile = fileName
	meta, err := NewFile(cfg)
	c.Assert(err, IsNil)
	defer os.RemoveAll(fileName)

	// zero (initial) CommitTs
	c.Assert(meta.TS(), Equals, int64(0))

	testTs := int64(1)
	// save ts
	err = meta.Save(testTs, 0)
	c.Assert(err, IsNil)
	// check ts
	ts := meta.TS()
	c.Assert(ts, Equals, testTs)

	// check load ts
	err = meta.Load()
	c.Assert(err, IsNil)
	ts = meta.TS()
	c.Assert(ts, Equals, testTs)

	// check not exist meta file
	cfg.CheckPointFile = notExistFileName
	meta, err = NewFile(cfg)
	c.Assert(err, IsNil)
	err = meta.Load()
	c.Assert(err, IsNil)
	c.Assert(meta.TS(), Equals, int64(0))

	// check not exist meta file, but with initialCommitTs
	cfg.InitialCommitTS = 123
	meta, err = NewFile(cfg)
	c.Assert(err, IsNil)
	c.Assert(meta.TS(), Equals, cfg.InitialCommitTS)

	// close the checkpoint
	err = meta.Close()
	c.Assert(err, IsNil)
	c.Assert(errors.Cause(meta.Load()), Equals, ErrCheckPointClosed)
	c.Assert(errors.Cause(meta.Save(0, 0)), Equals, ErrCheckPointClosed)
	c.Assert(errors.Cause(meta.Close()), Equals, ErrCheckPointClosed)
}
