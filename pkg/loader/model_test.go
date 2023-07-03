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
	"regexp"
	"strings"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"

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
	dml.DestDBType = TiDB

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
	args = dml.buildWhere(builder, 0)
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
	args = dml.buildWhere(builder, 0)
	c.Assert(args, check.DeepEquals, []interface{}{1, 1})
	c.Assert(strings.Count(builder.String(), "?"), check.Equals, len(args))

	// set a1 to NULL value
	values["a1"] = nil
	builder.Reset()
	args = dml.buildWhere(builder, 0)
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
		DestDBType: TiDB,
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
		DestDBType: TiDB,
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
		DestDBType: TiDB,
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

func (s *SQLSuite) TestOracleUpdateSQLCharType(c *check.C) {
	dml := DML{
		Tp:       UpdateDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"ID":      123,
			"NAME":    "pc",
			"OFFER":   "oo",
			"ADDRESS": "aa",
		},
		OldValues: map[string]interface{}{
			"ID":      123,
			"NAME":    "pingcap",
			"OFFER":   "o",
			"ADDRESS": "a",
		},
		info: &tableInfo{
			columns: []string{"ID", "NAME", "OFFER", "ADDRESS"},
			dataTypeMap: map[string]string{
				"ID":      "VARCHAR2",
				"NAME":    "VARCHAR2",
				"OFFER":   "CHAR",
				"ADDRESS": "NCHAR",
			},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"ID": {
				FieldType: *types.NewFieldType(mysql.TypeInt24),
			},
			"NAME": {
				FieldType: *types.NewFieldType(mysql.TypeVarString),
			},
			"OFFER": {
				FieldType: *types.NewFieldType(mysql.TypeVarString),
			},
			"ADDRESS": {
				FieldType: *types.NewFieldType(mysql.TypeVarString),
			},
		},
		DestDBType: OracleDB,
	}
	sql, args := dml.sql()
	c.Assert(
		sql, check.Equals,
		"UPDATE db.tbl SET ADDRESS = :1,ID = :2,NAME = :3,OFFER = :4 WHERE RTRIM(ADDRESS) = :5 AND ID = :6 AND NAME = :7 AND RTRIM(OFFER) = :8 AND rownum <=1")
	c.Assert(args, check.HasLen, 8)
	c.Assert(args[0], check.Equals, "aa")
	c.Assert(args[1], check.Equals, 123)
	c.Assert(args[2], check.Equals, "pc")
	c.Assert(args[3], check.Equals, "oo")
	c.Assert(args[4], check.Equals, "a")
	c.Assert(args[5], check.Equals, 123)
	c.Assert(args[6], check.Equals, "pingcap")
	c.Assert(args[7], check.Equals, "o")
}

func (s *SQLSuite) TestOracleUpdateSQL(c *check.C) {
	dml := DML{
		Tp:       UpdateDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"ID":   123,
			"NAME": "pc",
		},
		OldValues: map[string]interface{}{
			"ID":   123,
			"NAME": "pingcap",
		},
		info: &tableInfo{
			columns: []string{"ID", "NAME"},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"ID": {
				FieldType: *types.NewFieldType(mysql.TypeInt24)},
			"NAME": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
		},
		DestDBType: OracleDB,
	}
	sql, args := dml.sql()
	c.Assert(
		sql, check.Equals,
		"UPDATE db.tbl SET ID = :1,NAME = :2 WHERE ID = :3 AND NAME = :4 AND rownum <=1")
	c.Assert(args, check.HasLen, 4)
	c.Assert(args[0], check.Equals, 123)
	c.Assert(args[1], check.Equals, "pc")
	c.Assert(args[2], check.Equals, 123)
	c.Assert(args[3], check.Equals, "pingcap")
}

func (s *SQLSuite) TestOracleUpdateSQLEmptyString(c *check.C) {
	dml := DML{
		Tp:       UpdateDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"ID":    123,
			"NAME":  "pc",
			"OFFER": nil,
		},
		OldValues: map[string]interface{}{
			"ID":    123,
			"NAME":  "",
			"OFFER": nil,
		},
		info: &tableInfo{
			columns: []string{"ID", "NAME", "OFFER"},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"ID": {
				FieldType: *types.NewFieldType(mysql.TypeInt24)},
			"NAME": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
			"OFFER": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
		},
		DestDBType: OracleDB,
	}
	sql, args := dml.sql()
	c.Assert(
		sql, check.Equals,
		"UPDATE db.tbl SET ID = :1,NAME = :2,OFFER = :3 WHERE ID = :4 AND NAME IS NULL AND OFFER IS NULL AND rownum <=1")
	c.Assert(args, check.HasLen, 4)
	c.Assert(args[0], check.Equals, 123)
	c.Assert(args[1], check.Equals, "pc")
	c.Assert(args[2], check.Equals, nil)
	c.Assert(args[3], check.Equals, 123)
}

func (s *SQLSuite) TestOracleUpdateSQLPrimaryKey(c *check.C) {
	dml := DML{
		Tp:       UpdateDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"ID":   123,
			"NAME": "pc",
		},
		OldValues: map[string]interface{}{
			"ID":   123,
			"NAME": "pingcap",
		},
		info: &tableInfo{
			columns: []string{"ID", "NAME"},
			uniqueKeys: []indexInfo{
				{
					name:    "uniq name",
					columns: []string{"ID"},
				},
				{
					name:    "other",
					columns: []string{"OTHER"},
				},
			},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"ID": {
				FieldType: *types.NewFieldType(mysql.TypeInt24)},
			"NAME": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
		},
		DestDBType: OracleDB,
	}
	sql, args := dml.sql()
	c.Assert(
		sql, check.Equals,
		"UPDATE db.tbl SET ID = :1,NAME = :2 WHERE ID = :3 AND rownum <=1")
	c.Assert(args, check.HasLen, 3)
	c.Assert(args[0], check.Equals, 123)
	c.Assert(args[1], check.Equals, "pc")
	c.Assert(args[2], check.Equals, 123)
}

func (s *SQLSuite) TestOracleDeleteSQLCharType(c *check.C) {
	dml := DML{
		Tp:       DeleteDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"ID":      123,
			"NAME":    "pc",
			"OFFER":   "o",
			"ADDRESS": "a",
		},
		info: &tableInfo{
			columns: []string{"ID", "NAME", "OFFER", "ADDRESS"},
			dataTypeMap: map[string]string{
				"ID":      "VARCHAR2",
				"NAME":    "VARCHAR2",
				"OFFER":   "CHAR",
				"ADDRESS": "NCHAR",
			},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"ID": {
				FieldType: *types.NewFieldType(mysql.TypeInt24)},
			"NAME": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
			"OFFER": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
			"ADDRESS": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
		},
		DestDBType: OracleDB,
	}
	sql, args := dml.sql()
	c.Assert(
		sql, check.Equals,
		"DELETE FROM db.tbl WHERE RTRIM(ADDRESS) = :1 AND ID = :2 AND NAME = :3 AND RTRIM(OFFER) = :4 AND rownum <=1")
	c.Assert(args, check.HasLen, 4)
	c.Assert(args[0], check.Equals, "a")
	c.Assert(args[1], check.Equals, 123)
	c.Assert(args[2], check.Equals, "pc")
	c.Assert(args[3], check.Equals, "o")
}

func (s *SQLSuite) TestOracleDeleteSQL(c *check.C) {
	dml := DML{
		Tp:       DeleteDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"ID":   123,
			"NAME": "pc",
		},
		info: &tableInfo{
			columns: []string{"ID", "NAME"},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"ID": {
				FieldType: *types.NewFieldType(mysql.TypeInt24)},
			"NAME": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
		},
		DestDBType: OracleDB,
	}
	sql, args := dml.sql()
	c.Assert(
		sql, check.Equals,
		"DELETE FROM db.tbl WHERE ID = :1 AND NAME = :2 AND rownum <=1")
	c.Assert(args, check.HasLen, 2)
	c.Assert(args[0], check.Equals, 123)
	c.Assert(args[1], check.Equals, "pc")
}

func (s *SQLSuite) TestOracleDeleteSQLEmptyString(c *check.C) {
	dml := DML{
		Tp:       DeleteDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"ID":   123,
			"NAME": "",
		},
		info: &tableInfo{
			columns: []string{"ID", "NAME"},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"ID": {
				FieldType: *types.NewFieldType(mysql.TypeInt24)},
			"NAME": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
		},
		DestDBType: OracleDB,
	}
	sql, args := dml.sql()
	c.Assert(
		sql, check.Equals,
		"DELETE FROM db.tbl WHERE ID = :1 AND NAME IS NULL AND rownum <=1")
	c.Assert(args, check.HasLen, 1)
	c.Assert(args[0], check.Equals, 123)
}

func (s *SQLSuite) TestOracleInsertSQL(c *check.C) {
	dml := DML{
		Tp:       InsertDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"ID":   123,
			"NAME": "pc",
			"C2":   nil,
		},
		info: &tableInfo{
			columns: []string{"ID", "NAME", "C2"},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"ID": {
				FieldType: *types.NewFieldType(mysql.TypeInt24)},
			"NAME": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
			"C2": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
		},
		DestDBType: OracleDB,
	}
	sql, args := dml.sql()
	c.Assert(
		sql, check.Equals,
		"INSERT INTO db.tbl(C2,ID,NAME) VALUES(:1,:2,:3)")
	c.Assert(args, check.HasLen, 3)
	c.Assert(args[0], check.Equals, nil)
	c.Assert(args[1], check.Equals, 123)
	c.Assert(args[2], check.Equals, "pc")
}

func (s *SQLSuite) TestOracleDeleteNewValueSQLWithOneUK(c *check.C) {
	dml := DML{
		Tp:       InsertDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"ID":   123,
			"NAME": "pc",
			"C2":   nil,
		},
		info: &tableInfo{
			columns: []string{"ID", "NAME", "C2"},
			uniqueKeys: []indexInfo{
				{
					name:    "uniq_index_name",
					columns: []string{"ID"},
				},
			},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"ID": {
				FieldType: *types.NewFieldType(mysql.TypeInt24)},
			"NAME": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
			"C2": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
		},
		DestDBType: OracleDB,
	}

	sql, args := dml.oracleDeleteNewValueSQL()
	c.Assert(
		sql, check.Equals,
		"DELETE FROM db.tbl WHERE ID = :1 AND rownum <=1")
	c.Assert(args, check.HasLen, 1)
	c.Assert(args[0], check.Equals, 123)

	// column in UK have nil value, so fall back to all columns
	dml = DML{
		Tp:       InsertDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"ID":   123,
			"NAME": "pc",
			"C2":   nil,
		},
		info: &tableInfo{
			columns: []string{"ID", "NAME", "C2"},
			uniqueKeys: []indexInfo{
				{
					name:    "uniq_index_name",
					columns: []string{"ID", "C2"},
				},
			},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"ID": {
				FieldType: *types.NewFieldType(mysql.TypeInt24)},
			"NAME": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
			"C2": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
		},
		DestDBType: OracleDB,
	}
	sql, args = dml.oracleDeleteNewValueSQL()
	c.Assert(
		sql, check.Equals,
		"DELETE FROM db.tbl WHERE C2 IS NULL AND ID = :1 AND NAME = :2 AND rownum <=1")
	c.Assert(args, check.HasLen, 2)
	c.Assert(args[0], check.Equals, 123)
	c.Assert(args[1], check.Equals, "pc")
}

func (s *SQLSuite) TestOracleDeleteNewValueSQLWithMultiUK(c *check.C) {
	dml := DML{
		Tp:       InsertDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"ID":   123,
			"ID2":  "456",
			"NAME": "pc",
			"C2":   nil,
		},
		info: &tableInfo{
			columns: []string{"ID", "ID2", "NAME", "C2"},
			uniqueKeys: []indexInfo{
				{
					name:    "uniq_index_name_id",
					columns: []string{"ID", "C2"},
				},
				{
					name:    "uniq_index_name_id2",
					columns: []string{"ID2"},
				},
			},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"ID": {
				FieldType: *types.NewFieldType(mysql.TypeInt24)},
			"ID2": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
			"NAME": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
			"C2": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
		},
		DestDBType: OracleDB,
	}

	sql, args := dml.oracleDeleteNewValueSQL()
	c.Assert(
		sql, check.Equals,
		"DELETE FROM db.tbl WHERE ID2 = :1 AND rownum <=1")
	c.Assert(args, check.HasLen, 1)
	c.Assert(args[0], check.Equals, "456")
}

func (s *SQLSuite) TestOracleDeleteNewValueSQLWithNoUK(c *check.C) {
	dml := DML{
		Tp:       InsertDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"ID":   123,
			"ID2":  "456",
			"NAME": "pc",
			"C2":   nil,
		},
		info: &tableInfo{
			columns: []string{"ID", "ID2", "NAME", "C2"},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"ID": {
				FieldType: *types.NewFieldType(mysql.TypeInt24)},
			"ID2": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
			"NAME": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
			"C2": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
		},
		DestDBType: OracleDB,
	}

	sql, args := dml.oracleDeleteNewValueSQL()
	c.Assert(
		sql, check.Equals,
		"DELETE FROM db.tbl WHERE C2 IS NULL AND ID = :1 AND ID2 = :2 AND NAME = :3 AND rownum <=1")
	c.Assert(args, check.HasLen, 3)
	c.Assert(args[0], check.Equals, 123)
	c.Assert(args[1], check.Equals, "456")
	c.Assert(args[2], check.Equals, "pc")
}

func (s *SQLSuite) TestOracleDeleteNewValueSQLEmptyString(c *check.C) {
	dml := DML{
		Tp:       InsertDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"ID":   123,
			"ID2":  "456",
			"NAME": "",
			"C2":   nil,
		},
		info: &tableInfo{
			columns: []string{"ID", "ID2", "NAME", "C2"},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"ID": {
				FieldType: *types.NewFieldType(mysql.TypeInt24)},
			"ID2": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
			"NAME": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
			"C2": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
		},
		DestDBType: OracleDB,
	}

	sql, args := dml.oracleDeleteNewValueSQL()
	c.Assert(
		sql, check.Equals,
		"DELETE FROM db.tbl WHERE C2 IS NULL AND ID = :1 AND ID2 = :2 AND NAME IS NULL AND rownum <=1")
	c.Assert(args, check.HasLen, 2)
	c.Assert(args[0], check.Equals, 123)
	c.Assert(args[1], check.Equals, "456")
}

func (s *SQLSuite) TestOracleDeleteNewValueSQLCharType(c *check.C) {
	dml := DML{
		Tp:       InsertDMLType,
		Database: "db",
		Table:    "tbl",
		Values: map[string]interface{}{
			"ID":   123,
			"ID2":  "456",
			"NAME": "n",
			"C2":   "c",
		},
		info: &tableInfo{
			columns: []string{"ID", "ID2", "NAME", "C2"},
			dataTypeMap: map[string]string{
				"ID":   "VARCHAR2",
				"ID2":  "VARCHAR2",
				"NAME": "CHAR",
				"C2":   "NCHAR",
			},
		},
		UpColumnsInfoMap: map[string]*model.ColumnInfo{
			"ID": {
				FieldType: *types.NewFieldType(mysql.TypeInt24)},
			"ID2": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
			"NAME": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
			"C2": {
				FieldType: *types.NewFieldType(mysql.TypeVarString)},
		},
		DestDBType: OracleDB,
	}

	sql, args := dml.oracleDeleteNewValueSQL()
	c.Assert(
		sql, check.Equals,
		"DELETE FROM db.tbl WHERE RTRIM(C2) = :1 AND ID = :2 AND ID2 = :3 AND RTRIM(NAME) = :4 AND rownum <=1")
	c.Assert(args, check.HasLen, 4)
	c.Assert(args[0], check.Equals, "c")
	c.Assert(args[1], check.Equals, 123)
	c.Assert(args[2], check.Equals, "456")
	c.Assert(args[3], check.Equals, "n")
}
