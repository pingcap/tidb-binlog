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
	"strings"

	check "github.com/pingcap/check"
)

type dmlSuite struct {
}

var _ = check.Suite(&dmlSuite{})

func getDML(key bool, tp DMLType) *DML {
	info := &tableInfo{
		columns: []string{"id", "a1"},
	}

	if key {
		info.uniqueKeys = append(info.uniqueKeys, indexInfo{"PRIMARY", []string{"id"}})
	}

	dml := new(DML)
	dml.info = info
	dml.Database = "test"
	dml.Table = "test"
	dml.Tp = tp

	return dml
}

func (d *dmlSuite) TestWhere(c *check.C) {
	d.testWhere(c, InsertDMLType)
	d.testWhere(c, UpdateDMLType)
	d.testWhere(c, DeleteDMLType)
}

func (d *dmlSuite) testWhere(c *check.C, tp DMLType) {
	dml := getDML(true, tp)
	var values = map[string]interface{}{
		"id": 1,
		"a1": 1,
	}

	if tp == UpdateDMLType {
		dml.OldValues = values
	} else {
		dml.Values = values
	}

	names, args := dml.whereSlice()
	c.Assert(names, check.DeepEquals, []string{"id"})
	c.Assert(args, check.DeepEquals, []interface{}{1})

	builder := new(strings.Builder)
	args = dml.buildWhere(builder)
	c.Assert(args, check.DeepEquals, []interface{}{1})
	c.Assert(strings.Count(builder.String(), "?"), check.Equals, len(args))

	// no pk
	dml = getDML(false, tp)
	if tp == UpdateDMLType {
		dml.OldValues = values
	} else {
		dml.Values = values
	}

	names, args = dml.whereSlice()
	c.Assert(names, check.DeepEquals, []string{"id", "a1"})
	c.Assert(args, check.DeepEquals, []interface{}{1, 1})

	builder.Reset()
	args = dml.buildWhere(builder)
	c.Assert(args, check.DeepEquals, []interface{}{1, 1})
	c.Assert(strings.Count(builder.String(), "?"), check.Equals, len(args))

	// set a1 to NULL value
	values["a1"] = nil
	builder.Reset()
	args = dml.buildWhere(builder)
	c.Assert(args, check.DeepEquals, []interface{}{1})
	c.Assert(strings.Count(builder.String(), "?"), check.Equals, len(args))
}

type getKeysSuite struct{}

var _ = check.Suite(&getKeysSuite{})

func (s *getKeysSuite) TestGetKeyShouldUseNamesWithVals(c *check.C) {
	names := []string{"name", "age", "city"}
	values := map[string]interface{}{
		"name": "pingcap",
		"age":  42,
	}
	c.Assert(getKey(names, values), check.Equals, "(name: pingcap)(age: 42)")
}

func (s *getKeysSuite) TestShouldHaveAtLeastOneKey(c *check.C) {
	dml := DML{
		Tp: InsertDMLType,
		info: &tableInfo{
			columns: []string{"id", "name"},
		},
		Values: map[string]interface{}{
			"name": "tester",
		},
	}
	keys := getKeys(&dml)
	c.Assert(keys, check.HasLen, 1)
}

func (s *getKeysSuite) TestShouldCollectNewOldUniqKeyVals(c *check.C) {
	dml := DML{
		Database: "db",
		Table:    "tbl",
		Tp:       UpdateDMLType,
		info: &tableInfo{
			columns: []string{"id", "first", "last", "other"},
			uniqueKeys: []indexInfo{
				{
					name:    "uniq name",
					columns: []string{"first", "last"},
				},
				{
					name:    "other",
					columns: []string{"other"},
				},
			},
		},
		Values: map[string]interface{}{
			"first": "strict",
			"last":  "tester",
			"other": 42,
		},
		OldValues: map[string]interface{}{
			"first": "Strict",
			"last":  "Tester",
			"other": 1,
		},
	}
	keys := getKeys(&dml)
	expected := []string{
		"(first: strict)(last: tester)`db`.`tbl`",
		"(other: 42)`db`.`tbl`",
		"(first: Strict)(last: Tester)`db`.`tbl`",
		"(other: 1)`db`.`tbl`",
	}
	c.Assert(keys, check.DeepEquals, expected)
}

type SQLSuite struct{}

var _ = check.Suite(&SQLSuite{})

func (s *SQLSuite) TestInsertSQL(c *check.C) {
	dml := DML{
		Tp:       InsertDMLType,
		Database: "test",
		Table:    "hello",
		Values: map[string]interface{}{
			"name": "pc",
			"age":  42,
		},
		info: &tableInfo{
			columns: []string{"name", "age"},
		},
	}
	sql, args := dml.sql()
	c.Assert(sql, check.Equals, "INSERT INTO `test`.`hello`(`name`,`age`) VALUES(?,?)")
	c.Assert(args, check.HasLen, 2)
	c.Assert(args[0], check.Equals, "pc")
	c.Assert(args[1], check.Equals, 42)
}

func (s *SQLSuite) TestDeleteSQL(c *check.C) {
	dml := DML{
		Tp:       DeleteDMLType,
		Database: "test",
		Table:    "hello",
		Values: map[string]interface{}{
			"name": "pc",
		},
		info: &tableInfo{
			columns: []string{"name", "age"},
		},
	}
	sql, args := dml.sql()
	c.Assert(
		sql, check.Equals,
		"DELETE FROM `test`.`hello` WHERE `name` = ? AND `age` IS NULL LIMIT 1")
	c.Assert(args, check.HasLen, 1)
	c.Assert(args[0], check.Equals, "pc")
}

func (s *SQLSuite) TestUpdateSQL(c *check.C) {
	dml := DML{
		Tp:       UpdateDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"name": "pc",
		},
		OldValues: map[string]interface{}{
			"name": "pingcap",
		},
		info: &tableInfo{
			columns: []string{"name"},
		},
	}
	sql, args := dml.sql()
	c.Assert(
		sql, check.Equals,
		"UPDATE `db`.`tbl` SET `name` = ? WHERE `name` = ? LIMIT 1")
	c.Assert(args, check.HasLen, 2)
	c.Assert(args[0], check.Equals, "pc")
	c.Assert(args[1], check.Equals, "pingcap")
}
