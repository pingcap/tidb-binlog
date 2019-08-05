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
	"github.com/pingcap/check"
)

type syncerSuite struct{}

var _ = check.Suite(&syncerSuite{})

func (s *syncerSuite) TestIsIgnoreTxnCommitTS(c *check.C) {
	c.Assert(isIgnoreTxnCommitTS(nil, 1), check.IsFalse)
	c.Assert(isIgnoreTxnCommitTS([]int64{1, 3}, 1), check.IsTrue)
	c.Assert(isIgnoreTxnCommitTS([]int64{1, 3}, 2), check.IsFalse)
	c.Assert(isIgnoreTxnCommitTS([]int64{1, 3}, 3), check.IsTrue)
}
