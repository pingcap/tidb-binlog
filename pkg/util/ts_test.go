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
	"context"
	"errors"
	"time"

	. "github.com/pingcap/check"
	pd "github.com/pingcap/pd/client"
)

type tsSuite struct{}

var _ = Suite(&tsSuite{})

func (s *tsSuite) TestGetApproachTS(c *C) {
	c.Assert(GetApproachTS(0, time.Now()), Equals, int64(0))

	t := time.Now().Add(-1 * time.Second)
	ats := GetApproachTS(10, t)
	c.Assert(ats, Equals, int64(10+1000<<physicalShiftBits))
}

type dummyCli struct {
	pd.Client
	physical, logical int64
	err               error
}

func (c dummyCli) GetTS(ctx context.Context) (int64, int64, error) {
	return c.physical, c.logical, c.err
}

func (s *tsSuite) TestGetTSO(c *C) {
	t, err := GetTSO(dummyCli{err: errors.New("TSO")})
	c.Assert(t, Equals, int64(0))
	c.Assert(err, NotNil)

	t, err = GetTSO(dummyCli{physical: 1, logical: 10})
	c.Assert(err, IsNil)
	c.Assert(t, Equals, int64(1<<physicalShiftBits+10))
}

func (s *tsSuite) TestTSOToRoughTime(c *C) {
	t := TSOToRoughTime(407964913197645824)
	expectT := time.Date(2019, 4, 26, 15, 10, 38, 0, time.Local)
	c.Assert(t, Equals, expectT)
}
