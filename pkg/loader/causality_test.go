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

package loader

import (
	. "github.com/pingcap/check"
)

type causalitySuite struct{}

var _ = Suite(&causalitySuite{})

func (s *causalitySuite) TestCausality(c *C) {
	ca := NewCausality()
	caseData := []string{"test_1", "test_2", "test_3"}
	excepted := map[string]string{
		"test_1": "test_1",
		"test_2": "test_1",
		"test_3": "test_1",
	}
	c.Assert(ca.Add(caseData), IsNil)
	c.Assert(ca.relations, DeepEquals, excepted)
	c.Assert(ca.Add([]string{"test_4"}), IsNil)
	excepted["test_4"] = "test_4"
	c.Assert(ca.relations, DeepEquals, excepted)
	conflictData := []string{"test_4", "test_3"}
	c.Assert(ca.DetectConflict(conflictData), IsTrue)
	c.Assert(ca.Add(conflictData), NotNil)
	ca.Reset()
	c.Assert(ca.relations, HasLen, 0)
}
