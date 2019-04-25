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

package filter

import (
	"testing"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) { TestingT(t) }

type testFilterSuite struct {
}

var _ = Suite(&testFilterSuite{})

func (t *testFilterSuite) TestFilter(c *C) {
	DoDBs := []string{"fulldb", "~fulldb_re.*"}
	DoTables := []TableName{{"db", "table"}, {"db2", "~table"}}

	filter := NewFilter(nil, nil, DoDBs, DoTables)

	c.Assert(filter.SkipSchemaAndTable("Fulldb", "t1"), IsFalse)
	c.Assert(filter.SkipSchemaAndTable("fulldb_re_x", ""), IsFalse)
	c.Assert(filter.SkipSchemaAndTable("db", "table_skip"), IsTrue)
	c.Assert(filter.SkipSchemaAndTable("db2", "table"), IsFalse)

	// with ignore db
	filter = NewFilter([]string{"db2"}, nil, DoDBs, DoTables)
	c.Assert(filter.SkipSchemaAndTable("Fulldb", "t1"), IsFalse)
	c.Assert(filter.SkipSchemaAndTable("fulldb_re_x", ""), IsFalse)
	c.Assert(filter.SkipSchemaAndTable("db", "table_skip"), IsTrue)
	c.Assert(filter.SkipSchemaAndTable("db2", "table"), IsTrue)

	// with ignore table
	ignoreTables := []TableName{{"ignore", "ignore"}}
	filter = NewFilter(nil, ignoreTables, nil, nil)
	c.Assert(filter.SkipSchemaAndTable("ignore", "ignore"), IsTrue)
	c.Assert(filter.SkipSchemaAndTable("not_ignore", "not_ignore"), IsFalse)
}
