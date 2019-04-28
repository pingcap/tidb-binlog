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

package util

import (
	. "github.com/pingcap/check"
)

type httpSuite struct{}

var _ = Suite(&httpSuite{})

func (s *httpSuite) TestSuccessResponse(c *C) {
	resp := SuccessResponse("cool", 42)
	c.Assert(resp.Message, Equals, "cool")
	c.Assert(resp.Code, Equals, 200)
	c.Assert(resp.Data.(int), Equals, 42)
}

func (s *httpSuite) TestNotFoundResponsef(c *C) {
	resp := NotFoundResponsef("%d %s", 1984, "Hero")
	c.Assert(resp.Message, Equals, "1984 Hero not found")
	c.Assert(resp.Code, Equals, statusNotFound)
}

func (s *httpSuite) TestErrResponsef(c *C) {
	resp := ErrResponsef("doctor %d", 42)
	c.Assert(resp.Message, Equals, "doctor 42")
	c.Assert(resp.Code, Equals, statusOtherError)
}
