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
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	check "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

func Test(t *testing.T) { check.TestingT(t) }

type UtilSuite struct{}

var _ = check.Suite(&UtilSuite{})

func (cs *UtilSuite) SetUpTest(c *check.C) {
}

func (cs *UtilSuite) TestGetTableInfoTableNotExist(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	defer db.Close()

	// return empty rows
	columnRows := sqlmock.NewRows([]string{"Field", "Extra"})
	mock.ExpectQuery(regexp.QuoteMeta(colsSQL)).WithArgs("test", "test1").WillReturnRows(columnRows)

	_, err = getTableInfo(db, "test", "test1")
	c.Assert(errors.Cause(err), check.Equals, ErrTableNotExist)

}

func (cs *UtilSuite) TestGetTableInfo(c *check.C) {
	db, mock, err := sqlmock.New()

	c.Assert(err, check.IsNil)
	defer db.Close()

	// (id, a1, a2, a3, a4)
	// primary key: id
	// unique key: (a1) (a2,a3)
	columnRows := sqlmock.NewRows([]string{"Field", "Extra"}).
		AddRow("id", "").
		AddRow("a1", "").
		AddRow("a2", "").
		AddRow("a3", "VIRTUAL GENERATED").
		AddRow("a4", "")
	mock.ExpectQuery(regexp.QuoteMeta(colsSQL)).WithArgs("test", "test1").WillReturnRows(columnRows)

	indexRows := sqlmock.NewRows([]string{"non_unique", "index_name", "seq_in_index", "column_name"}).
		AddRow(0, "dex1", 1, "a1").
		AddRow(0, "PRIMARY", 1, "id").
		AddRow(0, "dex2", 1, "a2").
		AddRow(1, "dex3", 1, "a4").
		AddRow(0, "dex2", 2, "a3")

	mock.ExpectQuery(regexp.QuoteMeta(uniqKeysSQL)).WithArgs("test", "test1").WillReturnRows(indexRows)

	info, err := getTableInfo(db, "test", "test1")
	c.Assert(err, check.IsNil)
	c.Assert(info, check.NotNil)

	c.Assert(info, check.DeepEquals, &tableInfo{
		columns:    []string{"id", "a1", "a2", "a4"}, // generated column a3 is ignored
		primaryKey: &indexInfo{"PRIMARY", []string{"id"}},
		uniqueKeys: []indexInfo{{"PRIMARY", []string{"id"}},
			{"dex1", []string{"a1"}},
			{"dex2", []string{"a2", "a3"}},
		}})
}

func (cs *UtilSuite) TestGetOracleTableInfo(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)
	defer db.Close()

	columnRows := sqlmock.NewRows([]string{"column_name", "data_type"}).
		AddRow("C1", "VARCHAR2").
		AddRow("C2", "VARCHAR2").
		AddRow("C3", "VARCHAR2").
		AddRow("C4", "VARCHAR2").
		AddRow("C5", "VARCHAR2").
		AddRow("C6", "NUMBER").
		AddRow("C7", "CHAR").
		AddRow("C8", "NCHAR")
	mock.ExpectQuery(regexp.QuoteMeta(colsOracleSQL)).WithArgs("test", "t3").WillReturnRows(columnRows)

	indexRows := sqlmock.NewRows([]string{"index_type", "index_name", "column_position", "column_name"}).
		AddRow("NONUNIQUE", "T3_C7_C8_INDEX", 1, "C7").
		AddRow("NONUNIQUE", "T3_C7_C8_INDEX", 2, "C8").
		AddRow("PUNIQUE", "T3_PK", 1, "C1").
		AddRow("PUNIQUE", "T3_PK", 2, "C2").
		AddRow("UNIQUE", "T3_C3_C4_UINDEX", 1, "C3").
		AddRow("UNIQUE", "T3_C3_C4_UINDEX", 2, "C4").
		AddRow("UNIQUE", "T3_C5_C6_UINDEX", 1, "C5").
		AddRow("UNIQUE", "T3_C5_C6_UINDEX", 2, "C6")

	mock.ExpectQuery(regexp.QuoteMeta(uniqKeyOracleSQL)).WithArgs("test", "t3").WillReturnRows(indexRows)

	info, err := getOracleTableInfo(db, "test", "t3")
	c.Assert(err, check.IsNil)
	c.Assert(info, check.NotNil)

	c.Assert(info, check.DeepEquals, &tableInfo{
		columns:    []string{"C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8"},
		primaryKey: &indexInfo{name: "T3_PK", columns: []string{"C1", "C2"}},
		uniqueKeys: []indexInfo{
			{name: "T3_PK", columns: []string{"C1", "C2"}},
			{name: "T3_C3_C4_UINDEX", columns: []string{"C3", "C4"}},
			{name: "T3_C5_C6_UINDEX", columns: []string{"C5", "C6"}},
		},
		dataTypeMap: map[string]string{
			"C1": "VARCHAR2",
			"C2": "VARCHAR2",
			"C3": "VARCHAR2",
			"C4": "VARCHAR2",
			"C5": "VARCHAR2",
			"C6": "NUMBER",
			"C7": "CHAR",
			"C8": "NCHAR",
		},
	})

}

func (cs *UtilSuite) TestRemovePlacementSpecialComments(c *check.C) {
	cases := []struct {
		sql    string
		result string
		errMsg string
	}{
		{
			sql:    "CREATE TABLE `t1` (`a` INT) /*T![placement] PLACEMENT POLICY=`p1` */",
			result: "CREATE TABLE `t1` (`a` INT) ",
		},
		{
			sql:    "CREATE TABLE `t1` (`a` INT) /*T![placement] PLACEMENT POLICY=`p1` */ partition by range(a) (PARTITION `p0` VALUES LESS THAN (100) /*T![placement] PLACEMENT POLICY=`p3` */)",
			result: "CREATE TABLE `t1` (`a` INT)  PARTITION BY RANGE (`a`) (PARTITION `p0` VALUES LESS THAN (100) )",
		},
		{
			sql:    "CREATE TABLE `t1` (`a` INT) /*T![placement] PLACEMENT POLICY=`p1` */ /*T! SHARD_ROW_ID_BITS = 1 */",
			result: "CREATE TABLE `t1` (`a` INT)  /*T! SHARD_ROW_ID_BITS = 1 */",
		},
		{
			sql:    "ALTER TABLE `t1` /*T![placement] PLACEMENT POLICY=`p1` */ DEFAULT CHARACTER SET = UTF8MB4",
			result: "ALTER TABLE `t1`  DEFAULT CHARACTER SET = UTF8MB4",
		},
		{
			sql:    "ALTER TABLE `t1` /*T![placement] PLACEMENT POLICY=`p1` */ /*T! SHARD_ROW_ID_BITS = 1 */",
			result: "ALTER TABLE `t1`  /*T! SHARD_ROW_ID_BITS = 1 */",
		},
		{
			sql:    "ALTER TABLE `t1` /*T! SHARD_ROW_ID_BITS = 1 */ /*T![placement] PLACEMENT POLICY = `p1` */",
			result: "ALTER TABLE `t1` /*T! SHARD_ROW_ID_BITS = 1 */ ",
		},
		{
			// no placement sql should not change
			sql:    "create table t1(id int)",
			result: "create table t1(id int)",
		},
	}

	for _, ca := range cases {
		sql, err := removeDDLPlacementOptions(ca.sql)
		c.Assert(err, check.IsNil)
		c.Assert(sql, check.Equals, ca.result)
	}
}
