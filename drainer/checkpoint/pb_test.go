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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

func (t *testCheckPointSuite) TestPb(c *C) {
	fileName := "/tmp/test"
	notExistFileName := "test_not_exist"
	cfg := new(Config)
	cfg.CheckPointFile = fileName
	meta, err := NewPb(cfg)
	c.Assert(err, IsNil)
	defer os.RemoveAll(fileName)

	// zero (initial) CommitTs
	c.Assert(meta.TS(), Equals, int64(0))

	testTs := int64(1)
	// save ts
	err = meta.Save(testTs)
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
	meta, err = NewPb(cfg)
	c.Assert(err, IsNil)
	err = meta.Load()
	c.Assert(err, IsNil)
	c.Assert(meta.TS(), Equals, int64(0))

	// check not exist meta file, but with initialCommitTs
	cfg.InitialCommitTS = 123
	meta, err = NewPb(cfg)
	c.Assert(err, IsNil)
	c.Assert(meta.TS(), Equals, cfg.InitialCommitTS)

	// test for Check
	c.Assert(meta.Check(0), IsFalse)
	meta2, ok := meta.(*PbCheckPoint)
	c.Assert(ok, IsTrue)
	meta2.saveTime = meta2.saveTime.Add(-maxSaveTime - time.Second) // hack the `saveTime`
	c.Assert(meta.Check(0), IsTrue)

	// close the checkpoint
	err = meta.Close()
	c.Assert(err, IsNil)
	c.Assert(errors.Cause(meta.Load()), Equals, ErrCheckPointClosed)
	c.Assert(errors.Cause(meta.Save(0)), Equals, ErrCheckPointClosed)
	c.Assert(meta.Check(0), IsFalse)
	c.Assert(errors.Cause(meta.Close()), Equals, ErrCheckPointClosed)
}
