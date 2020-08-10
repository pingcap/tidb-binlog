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
	"time"

	. "github.com/pingcap/check"
)

type durationSuite struct{}

var _ = Suite(&durationSuite{})

func (s *durationSuite) TestParseDuration(c *C) {
	gc := Duration("7")
	expectDuration := 7 * 24 * time.Hour
	duration, err := gc.ParseDuration()
	c.Assert(err, IsNil)
	c.Assert(duration, Equals, expectDuration)

	gc = "30m"
	expectDuration = 30 * time.Minute
	duration, err = gc.ParseDuration()
	c.Assert(err, IsNil)
	c.Assert(duration, Equals, expectDuration)

	gc = "7d"
	_, err = gc.ParseDuration()
	c.Assert(err, NotNil)
}
