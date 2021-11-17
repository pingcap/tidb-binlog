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
	. "github.com/pingcap/check"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
)

type taskGroupSuite struct{}

var _ = Suite(&taskGroupSuite{})

/* May only get one log entry
func (s *taskGroupSuite) TestShouldRecoverFromPanic(c *C) {
	var logHook util.LogHook
	logHook.SetUp()
	defer logHook.TearDown()

	var called bool
	var g taskGroup
	g.GoNoPanic("test", func() {
		called = true
		panic("Evil Smile")
	})
	g.Wait()
	c.Assert(called, IsTrue)
	c.Assert(logHook.Entrys, HasLen, 2)
	c.Assert(logHook.Entrys[0].Message, Matches, ".*Recovered.*")
	c.Assert(logHook.Entrys[1].Message, Matches, ".*Exit.*")
}
*/

func (t *taskGroupSuite) TestCombineFilterRules(c *C) {
	filterRules := []*bf.BinlogEventRule{
		{
			Action:        bf.Ignore,
			SchemaPattern: "do_not_drop_database*",
			TablePattern:  "*",
			Events:        []bf.EventType{"drop database"},
			SQLPattern:    nil,
		},
		{
			Action:        bf.Ignore,
			SchemaPattern: "do_not_drop_database*",
			TablePattern:  "*",
			Events:        nil,
			SQLPattern:    []string{"alter table .* add column aaa int"},
		},
		{
			Action:        bf.Ignore,
			SchemaPattern: "do_not_drop_database*",
			TablePattern:  "*",
			Events:        []bf.EventType{"delete"},
			SQLPattern:    nil,
		},
		{
			Action:        bf.Ignore,
			SchemaPattern: "do_not_add_col_database*",
			TablePattern:  "do_not_add_col_table*",
			Events:        nil,
			SQLPattern:    []string{"alter table .* add column aaa int"},
		},
		{
			Action:        bf.Ignore,
			SchemaPattern: "do_not_delete_database*",
			TablePattern:  "do_not_delete_table*",
			Events:        []bf.EventType{"delete"},
			SQLPattern:    nil,
		},
	}
	expectRules := []*bf.BinlogEventRule{
		{
			Action:        bf.Ignore,
			SchemaPattern: "do_not_drop_database*",
			TablePattern:  "*",
			Events:        []bf.EventType{"drop database", "delete"},
			SQLPattern:    []string{"alter table .* add column aaa int"},
		},
		{
			Action:        bf.Ignore,
			SchemaPattern: "do_not_add_col_database*",
			TablePattern:  "do_not_add_col_table*",
			Events:        nil,
			SQLPattern:    []string{"alter table .* add column aaa int"},
		},
		{
			Action:        bf.Ignore,
			SchemaPattern: "do_not_delete_database*",
			TablePattern:  "do_not_delete_table*",
			Events:        []bf.EventType{"delete"},
			SQLPattern:    nil,
		},
	}
	c.Assert(expectRules, DeepEquals, combineFilterRules(filterRules))
}
