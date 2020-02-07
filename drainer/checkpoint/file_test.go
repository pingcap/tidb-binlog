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
	meta, err := NewFile(0, fileName)
	c.Assert(err, IsNil)
	defer os.RemoveAll(fileName)

	// zero (initial) CommitTs
	c.Assert(meta.TS(), Equals, int64(0))
	c.Assert(meta.Status(), Equals, StatusNormal)

	testTs := int64(1)
	// save ts
	err = meta.Save(testTs, 0, StatusRunning)
	c.Assert(err, IsNil)
	// check ts
	ts := meta.TS()
	c.Assert(ts, Equals, testTs)
	c.Assert(meta.Status(), Equals, StatusRunning)

	// check load ts
	err = meta.Load()
	c.Assert(err, IsNil)
	ts = meta.TS()
	c.Assert(ts, Equals, testTs)

	// check not exist meta file
	meta, err = NewFile(0, notExistFileName)
	c.Assert(err, IsNil)
	err = meta.Load()
	c.Assert(err, IsNil)
	c.Assert(meta.TS(), Equals, int64(0))

	// check not exist meta file, but with initialCommitTs
	var initialCommitTS int64 = 123
	meta, err = NewFile(initialCommitTS, notExistFileName)
	c.Assert(err, IsNil)
	c.Assert(meta.TS(), Equals, initialCommitTS)

	// close the checkpoint
	err = meta.Close()
	c.Assert(err, IsNil)
	c.Assert(errors.Cause(meta.Load()), Equals, ErrCheckPointClosed)
	c.Assert(errors.Cause(meta.Save(0, 0, StatusNormal)), Equals, ErrCheckPointClosed)
	c.Assert(errors.Cause(meta.Close()), Equals, ErrCheckPointClosed)
}
