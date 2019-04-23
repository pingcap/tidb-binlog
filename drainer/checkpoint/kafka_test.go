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
	"path/filepath"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

func (t *testCheckPointSuite) TestKafka(c *C) {
	dir := c.MkDir()
	fileName := filepath.Join(dir, "test_kafka")
	cfg := new(Config)
	cfg.CheckPointFile = fileName
	cp, err := newKafka(cfg)
	c.Assert(err, IsNil)
	c.Assert(cp.TS(), Equals, int64(0))

	testTs := int64(1)
	err = cp.Save(testTs)
	c.Assert(err, IsNil)
	ts := cp.TS()
	c.Assert(ts, Equals, testTs)

	// close the checkpoint
	err = cp.Close()
	c.Assert(err, IsNil)
	c.Assert(errors.Cause(cp.Load()), Equals, ErrCheckPointClosed)
	c.Assert(errors.Cause(cp.Save(0)), Equals, ErrCheckPointClosed)
	c.Assert(cp.Check(0), IsFalse)
	c.Assert(errors.Cause(cp.Close()), Equals, ErrCheckPointClosed)
}
