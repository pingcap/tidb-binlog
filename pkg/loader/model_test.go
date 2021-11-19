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
	"fmt"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"regexp"
	"strings"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"

	"github.com/pingcap/check"
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
		dml.Values = values
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
		dml.Values = values
	} else {
		dml.Values = values
	}

	names, args = dml.whereSlice()
	c.Assert(names, check.DeepEquals, []string{"a1", "id"})
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
	c.Assert(sql, check.Equals, "INSERT INTO `test`.`hello`(`age`,`name`) VALUES(?,?)")
	c.Assert(args, check.HasLen, 2)
	c.Assert(args[0], check.Equals, 42)
	c.Assert(args[1], check.Equals, "pc")
}

func (s *SQLSuite) TestDeleteSQL(c *check.C) {
	dml := DML{
		Tp:       DeleteDMLType,
		Database: "test",
		Table:    "hello",
		Values: map[string]interface{}{
			"name": "pc",
			"age":  10,
		},
		info: &tableInfo{
			columns: []string{"name", "age"},
		},
	}
	sql, args := dml.sql()
	c.Assert(
		sql, check.Equals,
		"DELETE FROM `test`.`hello` WHERE `age` = ? AND `name` = ? LIMIT 1")
	c.Assert(args, check.HasLen, 2)
	c.Assert(args[0], check.Equals, 10)
	c.Assert(args[1], check.Equals, "pc")
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

func (s *SQLSuite) TestUpdateMarkSQL(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	sql := fmt.Sprintf("update %s set %s=%s+1 where %s=? and %s=? limit 1;", loopbacksync.MarkTableName, loopbacksync.Val, loopbacksync.Val, loopbacksync.ID, loopbacksync.ChannelID)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(sql)).
		WithArgs(1, 100).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	e := newExecutor(db)
	info := &loopbacksync.LoopBackSync{ChannelID: 100, LoopbackControl: true, SyncDDL: true}
	e.info = info

	// begin will update the mark table if LoopbackControl is true.
	tx, err := e.begin()
	c.Assert(err, check.IsNil)

	err = tx.commit()
	c.Assert(err, check.IsNil)

	c.Assert(mock.ExpectationsWereMet(), check.IsNil)
}

func (s *SQLSuite) TestOracleUpdateSQL(c *check.C) {
	dml := DML{
		Tp:       UpdateDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"id":123,
			"name": "pc",
		},
		OldValues: map[string]interface{}{
			"id":123,
			"name": "pingcap",
		},
		info: &tableInfo{
			columns: []string{"id", "name"},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"id": {
				FieldType: types.FieldType{Tp: mysql.TypeInt24}},
			"name": {
				FieldType: types.FieldType{Tp: mysql.TypeVarString}},

		},
	}
	sql	:= dml.oracleSQL()
	c.Assert(
		sql, check.Equals,
		"UPDATE db.tbl SET id = 123,name = 'pc' WHERE id = 123 AND name = 'pingcap' AND rownum <=1")
}

func (s *SQLSuite) TestOracleUpdateSQLPrimaryKey(c *check.C) {
	dml := DML{
		Tp:       UpdateDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"id":123,
			"name": "pc",
		},
		OldValues: map[string]interface{}{
			"id":123,
			"name": "pingcap",
		},
		info: &tableInfo{
			columns: []string{"id", "name"},
			uniqueKeys: []indexInfo{
						{
							name:    "uniq name",
							columns: []string{"id"},
						},
						{
							name:    "other",
							columns: []string{"other"},
						},
						},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"id": {
				FieldType: types.FieldType{Tp: mysql.TypeInt24}},
			"name": {
				FieldType: types.FieldType{Tp: mysql.TypeVarString}},

		},
	}
	sql	:= dml.oracleSQL()
	c.Assert(
		sql, check.Equals,
		"UPDATE db.tbl SET id = 123,name = 'pc' WHERE id = 123 AND rownum <=1")
}

func (s *SQLSuite) TestOracleDeleteSQL(c *check.C) {
	dml := DML{
		Tp:       DeleteDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"id":123,
			"name": "pc",
		},
		info: &tableInfo{
			columns: []string{"id", "name"},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"id": {
				FieldType: types.FieldType{Tp: mysql.TypeInt24}},
			"name": {
				FieldType: types.FieldType{Tp: mysql.TypeVarString}},

		},
	}
	sql	:= dml.oracleSQL()
	c.Assert(
		sql, check.Equals,
		"DELETE FROM db.tbl WHERE id = 123 AND name = 'pc' AND rownum <=1")
}

func (s *SQLSuite) TestOracleInsertSQL(c *check.C) {
	dml := DML{
		Tp:       InsertDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"id":123,
			"name": "pc",
			"c2":nil,
		},
		info: &tableInfo{
			columns: []string{"id", "name", "c2"},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"id": {
				FieldType: types.FieldType{Tp: mysql.TypeInt24}},
			"name": {
				FieldType: types.FieldType{Tp: mysql.TypeVarString}},
			"c2": {
				FieldType: types.FieldType{Tp: mysql.TypeVarString}},

		},
	}
	sql	:= dml.oracleSQL()
	c.Assert(
		sql, check.Equals,
		"INSERT INTO db.tbl (c2, id, name) VALUES (NULL, 123, 'pc')")
}

func (s *SQLSuite) TestGenOracleValue(c *check.C) {
	columnInfo := model.ColumnInfo{
			FieldType: types.FieldType{Tp: mysql.TypeDate},
	}
	colVaue := "2021-09-13"
	val := genOracleValue(&columnInfo, colVaue)
	c.Assert(
		val, check.Equals,
		"TO_DATE('2021-09-13', 'yyyy-mm-dd')")

	columnInfo = model.ColumnInfo{
		FieldType: types.FieldType{Tp: mysql.TypeDatetime, Decimal: 0},
	}
	colVaue = "2021-09-13 10:10:23"
	val = genOracleValue(&columnInfo, colVaue)
	c.Assert(
		val, check.Equals,
		"TO_DATE('2021-09-13 10:10:23', 'yyyy-mm-dd hh24:mi:ss')")

	columnInfo = model.ColumnInfo{
		FieldType: types.FieldType{Tp: mysql.TypeDatetime, Decimal: 6},
	}
	colVaue = "2021-09-13 10:10:23.123456"
	val = genOracleValue(&columnInfo, colVaue)
	c.Assert(
		val, check.Equals,
		"TO_TIMESTAMP('2021-09-13 10:10:23.123456', 'yyyy-mm-dd hh24:mi:ss.ff6')")

	columnInfo = model.ColumnInfo{
		FieldType: types.FieldType{Tp: mysql.TypeTimestamp, Decimal: 5},
	}
	colVaue = "2021-09-13 10:10:23.12345"
	val = genOracleValue(&columnInfo, colVaue)
	c.Assert(
		val, check.Equals,
		"TO_TIMESTAMP('2021-09-13 10:10:23.12345', 'yyyy-mm-dd hh24:mi:ss.ff5')")

	columnInfo = model.ColumnInfo{
		FieldType: types.FieldType{Tp: mysql.TypeYear},
	}
	colVaue = "2021"
	val = genOracleValue(&columnInfo, colVaue)
	c.Assert(
		val, check.Equals,"2021")

	columnInfo = model.ColumnInfo{
		FieldType: types.FieldType{Tp: mysql.TypeVarchar},
	}
	colVaue = "2021"
	val = genOracleValue(&columnInfo, colVaue)
	c.Assert(
		val, check.Equals,"'2021'")

	columnInfo = model.ColumnInfo{
		FieldType: types.FieldType{Tp: mysql.TypeDuration},
	}
	colVaue = "23:11:59"
	val = genOracleValue(&columnInfo, colVaue)
	c.Assert(
		val, check.Equals,"TO_DATE('23:11:59', 'hh24:mi:ss')")

	var colVaue2 interface{}
	val = genOracleValue(&columnInfo, colVaue2)
	c.Assert(
		val, check.Equals,"NULL")
}