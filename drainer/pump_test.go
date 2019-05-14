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

package drainer

import (
	"context"
	. "github.com/pingcap/check"
)

type pumpSuite struct{}

var _ = Suite(&pumpSuite{})

func (s *pumpSuite) TestGetCompressorName(c *C) {
	ctx := context.Background()
	_, ok := getCompressorName(ctx)
	c.Assert(ok, IsFalse)

	ctx = context.WithValue(ctx, drainerKeyType("compressor"), 42)
	_, ok = getCompressorName(ctx)
	c.Assert(ok, IsFalse)

	ctx = context.WithValue(ctx, drainerKeyType("compressor"), "")
	_, ok = getCompressorName(ctx)
	c.Assert(ok, IsFalse)

	ctx = context.WithValue(ctx, drainerKeyType("compressor"), "gzip")
	cp, ok := getCompressorName(ctx)
	c.Assert(ok, IsTrue)
	c.Assert(cp, Equals, "gzip")
}
