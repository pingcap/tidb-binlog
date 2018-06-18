package translator

import (
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/codec"
)

var tidbRowID = int64(11)

func genCommitTS(i int) int64 {
	return int64(100 + i)
}

func (t *testTranslatorSuite) TestFlashGenInsertSQLs(c *C) {
	f := testGenFlashTranslator(c)
	schema := "T"
	tables := []*model.TableInfo{testGenTable("normal"), testGenTable("hasPK"), testGenTable("hasID")}
	expectedValCounts := []int{7, 7, 6}
	expectedSQLs := []string{
		"IMPORT INTO `t`.`account` (`id`,`name`,`sex`,`" + implicitColName + "`,`" + internalVersionColName + "`,`" + internalDelmarkColName + "`) values (?,?,?,?,?,?);",
		"IMPORT INTO `t`.`account` (`id`,`name`,`sex`,`" + implicitColName + "`,`" + internalVersionColName + "`,`" + internalDelmarkColName + "`) values (?,?,?,?,?,?);",
		"IMPORT INTO `t`.`account` (`id`,`name`,`sex`,`" + internalVersionColName + "`,`" + internalDelmarkColName + "`) values (?,?,?,?,?);",
	}
	for i, table := range tables {
		rowDatas, expected := testFlashGenRowData(c, table, i, false)
		binlog := testFlashGenInsertBinlog(c, table, rowDatas)
		sqls, keys, vals, err := f.GenInsertSQLs(schema, table, [][]byte{binlog}, genCommitTS(i))
		if fmt.Sprintf("%v", keys[0]) != fmt.Sprintf("[%s]", table.Name.O) {
			c.Assert(fmt.Sprintf("%v", keys[0]), Equals, "[]")
		}
		c.Assert(err, IsNil)
		c.Assert(len(vals[0]), Equals, expectedValCounts[i])
		c.Assert(sqls[0], Equals, expectedSQLs[i])
		for index := range vals[0] {
			c.Assert(vals[0][index], DeepEquals, expected[index])
		}
	}

	table := testGenTable("normal")
	rowDatas, _ := testFlashGenRowData(c, table, 1, false)
	binlog := testFlashGenInsertBinlog(c, table, rowDatas)
	_, _, _, err := f.GenInsertSQLs(schema, tables[0], [][]byte{binlog[6:]}, 0)
	c.Assert(err, NotNil)
}

func (t *testTranslatorSuite) TestGenUpdateFlashSQLs(c *C) {
	f := testGenFlashTranslator(c)
	schema := "T"
	tables := []*model.TableInfo{
		testGenTable("normal"),
		testGenTable("hasPK"),
		// TODO: Will add this table back when update supports changing primary key.
		// testGenTable("hasID"),
	}
	expectedValCounts := []int{7, 7, 6}
	expectedSQLs := []string{
		"IMPORT INTO `t`.`account` (`id`,`name`,`sex`,`" + implicitColName + "`,`" + internalVersionColName + "`,`" + internalDelmarkColName + "`) values (?,?,?,?,?,?);",
		"IMPORT INTO `t`.`account` (`id`,`name`,`sex`,`" + implicitColName + "`,`" + internalVersionColName + "`,`" + internalDelmarkColName + "`) values (?,?,?,?,?,?);",
		"IMPORT INTO `t`.`account` (`id`,`name`,`sex`,`" + internalVersionColName + "`,`" + internalDelmarkColName + "`) values (?,?,?,?,?);",
	}
	for i, table := range tables {
		oldRowDatas, _ := testFlashGenRowData(c, table, 1, false)
		newRowDatas, newExpected := testFlashGenRowData(c, table, i, false)
		binlog := testFlashGenUpdateBinlog(c, table, oldRowDatas, newRowDatas)
		sqls, keys, vals, err := f.GenUpdateSQLs(schema, table, [][]byte{binlog}, genCommitTS(i))
		if fmt.Sprintf("%v", keys[0]) != fmt.Sprintf("[%s]", table.Name.O) {
			c.Assert(fmt.Sprintf("%v", keys[0]), Equals, "[]")
		}
		c.Assert(err, IsNil)
		c.Assert(len(vals[0]), Equals, expectedValCounts[i])
		c.Assert(sqls[0], Equals, expectedSQLs[i])
		for index := range vals[0] {
			c.Assert(vals[0][index], DeepEquals, newExpected[index])
		}
	}

	table := testGenTable("normal")
	rowDatas, _ := testFlashGenRowData(c, table, 1, false)
	binlog := testFlashGenUpdateBinlog(c, table, rowDatas, rowDatas)
	_, _, _, err := f.GenUpdateSQLs(schema, table, [][]byte{binlog[6:]}, 0)
	c.Assert(err, NotNil)
}

func (t *testTranslatorSuite) TestFlashGenDeleteSQLs(c *C) {
	f := testGenFlashTranslator(c)
	schema := "T"
	tables := []*model.TableInfo{testGenTable("normal"), testGenTable("hasPK"), testGenTable("hasID")}
	expectedValCounts := []int{7, 7, 6}
	expectedSQLs := []string{
		"IMPORT INTO `t`.`account` (`id`,`name`,`sex`,`" + implicitColName + "`,`" + internalVersionColName + "`,`" + internalDelmarkColName + "`) values (?,?,?,?,?,?);",
		"IMPORT INTO `t`.`account` (`id`,`name`,`sex`,`" + implicitColName + "`,`" + internalVersionColName + "`,`" + internalDelmarkColName + "`) values (?,?,?,?,?,?);",
		"IMPORT INTO `t`.`account` (`id`,`name`,`sex`,`" + internalVersionColName + "`,`" + internalDelmarkColName + "`) values (?,?,?,?,?);",
	}
	for i, t := range tables {
		rowDatas, expected := testFlashGenRowData(c, t, i, true)
		binlog := testFlashGenDeleteBinlog(c, t, rowDatas)
		sqls, keys, vals, err := f.GenDeleteSQLs(schema, t, [][]byte{binlog}, genCommitTS(i))
		if fmt.Sprintf("%v", keys[0]) != fmt.Sprintf("[%s]", t.Name.O) {
			c.Assert(fmt.Sprintf("%v", keys[0]), Equals, "[]")
		}
		c.Assert(err, IsNil)
		c.Assert(len(vals[0]), Equals, expectedValCounts[i])
		c.Assert(sqls[0], Equals, expectedSQLs[i])
		for index := range vals[0] {
			c.Assert(vals[0][index], DeepEquals, expected[index])
		}
	}

	table := testGenTable("normal")
	rowDatas, _ := testFlashGenRowData(c, table, 1, true)
	binlog := testFlashGenDeleteBinlog(c, table, rowDatas)
	_, _, _, err := f.GenDeleteSQLs(schema, table, [][]byte{binlog[6:]}, 0)
	c.Assert(err, NotNil)
}

func (t *testTranslatorSuite) TestFlashGenDDLSQL(c *C) {
	f := testGenFlashTranslator(c)
	schema := "Test_Schema"
	dtRegex := "[0-9]{4}-[0-1][0-9]-[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9]"
	check := func(ddl string, checker Checker, expected string) {
		gen, err := f.GenDDLSQL(ddl, schema, 0)
		c.Assert(err, IsNil)
		c.Assert(gen, checker, expected)
	}

	check("create database "+schema,
		Equals,
		"CREATE DATABASE IF NOT EXISTS `test_schema`;")

	check("drop database "+schema,
		Equals,
		"DROP DATABASE `test_schema`;")

	// Primary keys.
	check("create table Test(I int, f float)",
		Equals,
		"CREATE TABLE IF NOT EXISTS `test_schema`.`test` (`"+implicitColName+"` Int64,`i` Nullable(Int32),`f` Nullable(Float32)) ENGINE MutableMergeTree((`"+implicitColName+"`), 8192);")
	check("create table Test(I int, f float, primary key(i))",
		Equals,
		"CREATE TABLE IF NOT EXISTS `test_schema`.`test` (`i` Int32,`f` Nullable(Float32)) ENGINE MutableMergeTree((`i`), 8192);")
	check("create table Test(I int primary key, f float not null)",
		Equals,
		"CREATE TABLE IF NOT EXISTS `test_schema`.`test` (`i` Int32,`f` Float32) ENGINE MutableMergeTree((`i`), 8192);")
	check("create table Test(I int, f float, primary key(i, f))",
		Equals,
		"CREATE TABLE IF NOT EXISTS `test_schema`.`test` (`"+implicitColName+"` Int64,`i` Nullable(Int32),`f` Nullable(Float32)) ENGINE MutableMergeTree((`"+implicitColName+"`), 8192);")
	// Numeric types, with unsigned, nullable and default value attributes.
	check("create table Test(bT bit, I int, T tinyint, M mediumint, B bigint, F float, D double, DE decimal)",
		Equals,
		"CREATE TABLE IF NOT EXISTS `test_schema`.`test` (`"+implicitColName+"` Int64,`bt` Nullable(UInt64),`i` Nullable(Int32),`t` Nullable(Int8),`m` Nullable(Int32),`b` Nullable(Int64),`f` Nullable(Float32),`d` Nullable(Float64),`de` Nullable(Float64)) ENGINE MutableMergeTree((`"+implicitColName+"`), 8192);")
	check("create table Test(I int unsigned, T tinyint unsigned, M mediumint unsigned, B bigint unsigned, F float unsigned, D double unsigned, DE decimal unsigned)",
		Equals,
		"CREATE TABLE IF NOT EXISTS `test_schema`.`test` (`"+implicitColName+"` Int64,`i` Nullable(UInt32),`t` Nullable(UInt8),`m` Nullable(UInt32),`b` Nullable(UInt64),`f` Nullable(Float32),`d` Nullable(Float64),`de` Nullable(Float64)) ENGINE MutableMergeTree((`"+implicitColName+"`), 8192);")
	check("create table Test(BT bit not null, I int not null, T tinyint not null, M mediumint not null, B bigint not null, F float not null, D double not null, DE decimal not null)",
		Equals,
		"CREATE TABLE IF NOT EXISTS `test_schema`.`test` (`"+implicitColName+"` Int64,`bt` UInt64,`i` Int32,`t` Int8,`m` Int32,`b` Int64,`f` Float32,`d` Float64,`de` Float64) ENGINE MutableMergeTree((`"+implicitColName+"`), 8192);")
	check("create table Test(Bt bit default 255, I int default null, T tinyint unsigned default 1, M mediumint not null default -2.0, B bigint unsigned not null default 100, F float not null default 1234.56, D double not null default 8765.4321, DE decimal not null default 0)",
		Equals,
		"CREATE TABLE IF NOT EXISTS `test_schema`.`test` (`"+implicitColName+"` Int64,`bt` Nullable(UInt64) DEFAULT 255,`i` Nullable(Int32) DEFAULT NULL,`t` Nullable(UInt8) DEFAULT 1,`m` Int32 DEFAULT -2.0,`b` UInt64 DEFAULT 100,`f` Float32 DEFAULT 1234.56,`d` Float64 DEFAULT 8765.4321,`de` Float64 DEFAULT 0) ENGINE MutableMergeTree((`"+implicitColName+"`), 8192);")
	check("create table Test(F float not null default 1234, D double not null default '8765.4321', DE decimal not null default 42)",
		Equals,
		"CREATE TABLE IF NOT EXISTS `test_schema`.`test` (`"+implicitColName+"` Int64,`f` Float32 DEFAULT 1234,`d` Float64 DEFAULT 8765.4321,`de` Float64 DEFAULT 42) ENGINE MutableMergeTree((`"+implicitColName+"`), 8192);")
	// String types, with default value attribute.
	check("create table Test(C Char(10) not null, vC Varchar(255) not null, B BLOB not null, t tinyblob not null, m MediumBlob not null, L longblob not null)",
		Equals,
		"CREATE TABLE IF NOT EXISTS `test_schema`.`test` (`"+implicitColName+"` Int64,`c` String,`vc` String,`b` String,`t` String,`m` String,`l` String) ENGINE MutableMergeTree((`"+implicitColName+"`), 8192);")
	check("create table Test(C Char(10) default NULL, vC Varchar(255) not null default 'abc', B BLOB not null default \"\", t tinyblob not null default '', m MediumBlob not null default \"def\", L longblob not null default 1234.5)",
		Equals,
		"CREATE TABLE IF NOT EXISTS `test_schema`.`test` (`"+implicitColName+"` Int64,`c` Nullable(String) DEFAULT NULL,`vc` String DEFAULT 'abc',`b` String DEFAULT '',`t` String DEFAULT '',`m` String DEFAULT 'def',`l` String DEFAULT '1234.5') ENGINE MutableMergeTree((`"+implicitColName+"`), 8192);")
	// Date/time types, with default value attribute.
	check("create table Test(DT Date not null, tM Time not null, dTTm DateTime not null, ts timestamp not null, y year not null)",
		Equals,
		"CREATE TABLE IF NOT EXISTS `test_schema`.`test` (`"+implicitColName+"` Int64,`dt` Date,`tm` Int64,`dttm` DateTime,`ts` DateTime,`y` Int16) ENGINE MutableMergeTree((`"+implicitColName+"`), 8192);")
	check("create table Test(DT Date not null default '0000-00-00', tM Time default -1, dTTm DateTime not null default \"2018-01-01 13:13:13\", ts timestamp not null default current_timestamp(), y year not null default +1984)",
		Matches,
		"CREATE TABLE IF NOT EXISTS `test_schema`.`test` \\(`"+implicitColName+"` Int64,`dt` Date DEFAULT '0000-00-00',`tm` Nullable\\(Int64\\) DEFAULT -1,`dttm` DateTime DEFAULT '2018-01-01 13:13:13',`ts` DateTime DEFAULT '"+dtRegex+"',`y` Int16 DEFAULT 1984\\) ENGINE MutableMergeTree\\(\\(`"+implicitColName+"`\\), 8192\\);")
	// Enum type, with default value attribute.
	check("create table Test(E Enum('a', 'b') not null)",
		Equals,
		"CREATE TABLE IF NOT EXISTS `test_schema`.`test` (`"+implicitColName+"` Int64,`e` Enum16(''=0,'a'=1,'b'=2)) ENGINE MutableMergeTree((`"+implicitColName+"`), 8192);")
	check("create table Test(E Enum('a', '', 'b') not null)",
		Equals,
		"CREATE TABLE IF NOT EXISTS `test_schema`.`test` (`"+implicitColName+"` Int64,`e` Enum16('a'=1,''=2,'b'=3)) ENGINE MutableMergeTree((`"+implicitColName+"`), 8192);")

	// Default value conversions using alter table statement, as the result is relatively simpler.
	// Bit.
	check("alter table Test add column bT bit default null",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `bt` Nullable(UInt64) DEFAULT NULL;")
	check("alter table Test add column bT bit not null default 255",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `bt` UInt64 DEFAULT 255;")
	check("alter table Test add column bT bit not null default -255",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `bt` UInt64 DEFAULT -255;")
	check("alter table Test add column bT bit not null default '255'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `bt` UInt64 DEFAULT 3290421;")
	check("alter table Test add column bT bit not null default 255.5",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `bt` UInt64 DEFAULT 256;")
	// Numeric.
	check("alter table Test add column i int default null",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `i` Nullable(Int32) DEFAULT NULL;")
	check("alter table Test add column i int not null default 255.5",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `i` Int32 DEFAULT 255.5;")
	check("alter table Test add column i int not null default -255",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `i` Int32 DEFAULT -255;")
	check("alter table Test add column i int not null default '255'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `i` Int32 DEFAULT 255;")
	check("alter table Test add column i int not null default '-255'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `i` Int32 DEFAULT -255;")
	// String.
	check("alter table Test add column vc varchar(10) default null",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `vc` Nullable(String) DEFAULT NULL;")
	check("alter table Test add column vc varchar(10) not null default 255",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `vc` String DEFAULT '255';")
	check("alter table Test add column vc varchar(10) not null default -255",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `vc` String DEFAULT '-255';")
	check("alter table Test add column vc varchar(10) not null default '\\''",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `vc` String DEFAULT '\\'';")
	// Date.
	check("alter table Test add column dt date default NULL",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` Nullable(Date) DEFAULT NULL;")
	check("alter table Test add column dt date not null default 19980808",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` Date DEFAULT '1998-08-08';")
	check("alter table Test add column dt date not null default 19980808232323",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` Date DEFAULT '1998-08-08 23:23:23';")
	check("alter table Test add column dt date not null default 19980808235959.99",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` Date DEFAULT '1998-08-09';")
	check("alter table Test add column dt date not null default 0",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` Date DEFAULT '0000-00-00';")
	check("alter table Test add column dt date not null default '1998-08-08'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` Date DEFAULT '1998-08-08';")
	check("alter table Test add column dt date not null default '1998-08-08 13:13:13'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` Date DEFAULT '1998-08-08';")
	check("alter table Test add column dt date not null default '1998-08-08 13:13'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` Date DEFAULT '1998-08-08';")
	check("alter table Test add column dt date not null default '1998-08-08 155:13:13'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` Date DEFAULT '1998-08-08';")
	check("alter table Test add column dt date not null default '19980808235959.99'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` Date DEFAULT '1998-08-09';")
	// Datetime and timestamp.
	check("alter table Test add column dt datetime not null default 19980101",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` DateTime DEFAULT '1998-01-01 00:00:00';")
	check("alter table Test add column dt datetime not null default 19980101232323",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` DateTime DEFAULT '1998-01-01 23:23:23';")
	check("alter table Test add column dt datetime not null default 19980101235959.999",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` DateTime DEFAULT '1998-01-02 00:00:00';")
	check("alter table Test add column dt datetime not null default 0",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` DateTime DEFAULT '0000-00-00 00:00:00';")
	check("alter table Test add column dt datetime not null default '0000-00-00'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` DateTime DEFAULT '0000-00-00 00:00:00';")
	check("alter table Test add column dt datetime not null default '0000-00-00 00:00:00'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` DateTime DEFAULT '0000-00-00 00:00:00';")
	check("alter table Test add column dt datetime not null default '1998-08-08'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` DateTime DEFAULT '1998-08-08 00:00:00';")
	check("alter table Test add column dt datetime not null default '1998-08-08 13:13'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` DateTime DEFAULT '1998-08-08 13:13:00';")
	check("alter table Test add column dt datetime not null default '1998-08-08 13:13:13'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `dt` DateTime DEFAULT '1998-08-08 13:13:13';")
	// Current_timestamp for Timestamp.
	check("alter table Test add column ts timestamp not null default current_timestamp'",
		Matches,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `ts` DateTime DEFAULT '"+dtRegex+"';")
	check("alter table Test add column ts timestamp not null default current_timestamp()'",
		Matches,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `ts` DateTime DEFAULT '"+dtRegex+"';")
	// Time(duration).
	check("alter table Test add column t time not null default 0",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `t` Int64 DEFAULT 0;")
	check("alter table Test add column t time not null default '0'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `t` Int64 DEFAULT 0;")
	check("alter table Test add column t time not null default -0",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `t` Int64 DEFAULT -0;")
	check("alter table Test add column t time not null default -10",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `t` Int64 DEFAULT -10;")
	check("alter table Test add column t time not null default '-10'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `t` Int64 DEFAULT -10;")
	check("alter table Test add column t time not null default 151515",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `t` Int64 DEFAULT 151515;")
	check("alter table Test add column t time not null default -151515",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `t` Int64 DEFAULT -151515;")
	check("alter table Test add column t time not null default 151515.99",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `t` Int64 DEFAULT 151516;")
	check("alter table Test add column t time not null default -151515.99",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `t` Int64 DEFAULT -151516;")
	check("alter table Test add column t time not null default '-151515.99'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `t` Int64 DEFAULT -151516;")
	// Year.
	check("alter table Test add column y year not null default 0",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `y` Int16 DEFAULT 2000;")
	check("alter table Test add column y year not null default '0'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `y` Int16 DEFAULT 2000;")
	check("alter table Test add column y year not null default -0",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `y` Int16 DEFAULT 2000;")
	check("alter table Test add column y year not null default '-0'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `y` Int16 DEFAULT 2000;")
	check("alter table Test add column y year not null default 10",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `y` Int16 DEFAULT 2010;")
	check("alter table Test add column y year not null default 1998",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `y` Int16 DEFAULT 1998;")
	check("alter table Test add column y year not null default 1998.0",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `y` Int16 DEFAULT 1998;")
	check("alter table Test add column y year not null default 1998.9",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `y` Int16 DEFAULT 1998;")
	check("alter table Test add column y year not null default '1998'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `y` Int16 DEFAULT 1998;")
	check("alter table Test add column y year not null default '1998.9'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `y` Int16 DEFAULT 1998;")
	check("alter table Test add column y year not null default '-1998.9'",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `y` Int16 DEFAULT 1998;")
	// Enum.
	check("alter table Test add column e enum('abc', 'def') not null default 'abc';",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `e` Enum16(''=0,'abc'=1,'def'=2) DEFAULT 'abc';")
	check("alter table Test add column e enum('abc', 'def') not null default 1;",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `e` Enum16(''=0,'abc'=1,'def'=2) DEFAULT 1;")
	// Set.
	check("alter table Test add column s set('abc', 'def') not null default 'def,abc,abc';",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `s` String DEFAULT 'abc,def';")
	check("alter table Test add column s set('abc', 'def') not null default 3;",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `s` String DEFAULT 'abc,def';")
	// JSON.
	check("alter table Test add column j json;",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `j` Nullable(String);")

	check("drop table Test",
		Equals,
		"DROP TABLE `test_schema`.`test`;")

	check("rename table Test to Test1",
		Equals,
		"RENAME TABLE `test_schema`.`test` TO `test_schema`.`test1`;")
	check("rename table Test to test_Schema1.Test1",
		Equals,
		"RENAME TABLE `test_schema`.`test` TO `test_schema1`.`test1`;")

	check("alter table Test rename Test1",
		Equals,
		"RENAME TABLE `test_schema`.`test` TO `test_schema`.`test1`;")
	check("alter table Test rename test_Schema1.Test1",
		Equals,
		"RENAME TABLE `test_schema`.`test` TO `test_schema1`.`test1`;")
	check("alter table Test add column C Int default 100",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `c` Nullable(Int32) DEFAULT 100;")
	check("alter table Test add column C varchar(255) default 100 after I",
		Equals,
		"ALTER TABLE `test_schema`.`test` ADD COLUMN `c` Nullable(String) DEFAULT '100' AFTER `i`;")
}

func (t *testTranslatorSuite) TestFlashFormatData(c *C) {
	check := func(tp byte, data types.Datum, expected interface{}) {
		value, err := formatFlashData(data, types.FieldType{Tp: tp})
		c.Assert(err, IsNil)
		c.Assert(value, DeepEquals, expected)
	}
	var datum types.Datum
	// Int types.
	check(mysql.TypeTiny, types.NewIntDatum(101), int8(101))
	check(mysql.TypeShort, types.NewIntDatum(101), int16(101))
	check(mysql.TypeInt24, types.NewIntDatum(101), int32(101))
	check(mysql.TypeLong, types.NewIntDatum(101), int32(101))
	check(mysql.TypeLonglong, types.NewIntDatum(101), int64(101))
	check(mysql.TypeFloat, types.NewFloat32Datum(101.101), float32(101.101))
	check(mysql.TypeDouble, types.NewFloat64Datum(101.101), float64(101.101))
	// Bit.
	bl, err := types.ParseBitStr("b101")
	c.Assert(err, IsNil)
	check(mysql.TypeBit, types.NewMysqlBitDatum(bl), uint64(5))
	// Duration.
	d, err := types.ParseDuration("101:10:11", 1)
	c.Assert(err, IsNil)
	check(mysql.TypeDuration, types.NewDurationDatum(d), int64(1011011))
	// Time types.
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	dt, err := types.ParseDate(sc, "0000-00-00")
	c.Assert(err, IsNil)
	check(mysql.TypeDate, types.NewTimeDatum(dt), int64(0))
	check(mysql.TypeDatetime, types.NewTimeDatum(dt), int64(0))
	check(mysql.TypeNewDate, types.NewTimeDatum(dt), int64(0))
	check(mysql.TypeTimestamp, types.NewTimeDatum(dt), int64(0))
	now := time.Now()
	utc := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), time.UTC)
	check(mysql.TypeDate, types.NewTimeDatum(types.Time{Time: types.FromGoTime(now)}), utc.Unix())
	check(mysql.TypeDatetime, types.NewTimeDatum(types.Time{Time: types.FromGoTime(now)}), now.Unix())
	check(mysql.TypeNewDate, types.NewTimeDatum(types.Time{Time: types.FromGoTime(now)}), utc.Unix())
	check(mysql.TypeTimestamp, types.NewTimeDatum(types.Time{Time: types.FromGoTime(now)}), now.Unix())
	// Decimal.
	check(mysql.TypeDecimal, types.NewDecimalDatum(types.NewDecFromFloatForTest(101.101)), float64(101.101))
	check(mysql.TypeNewDecimal, types.NewDecimalDatum(types.NewDecFromFloatForTest(101.101)), float64(101.101))
	// Enum.
	en, err := types.ParseEnumValue([]string{"a", "b"}, 1)
	c.Assert(err, IsNil)
	datum.SetMysqlEnum(en)
	check(mysql.TypeEnum, datum, int16(1))
	// Set.
	s, err := types.ParseSetName([]string{"a", "b"}, "a")
	c.Assert(err, IsNil)
	datum.SetMysqlSet(s)
	check(mysql.TypeSet, datum, "a")
	// JSON.
	datum.SetMysqlJSON(json.CreateBinary(uint64(101)))
	check(mysql.TypeJSON, datum, "101")
}

func testFlashGenRowData(c *C, table *model.TableInfo, base int, delFlag bool) ([]types.Datum, []interface{}) {
	datas := make([]types.Datum, 3)
	expected := make([]interface{}, 3)
	var pk interface{}
	for index, col := range table.Columns {
		d, e := testFlashGenDatum(c, col, base%2+1)
		datas[index] = d
		expected[index] = e
		// Only obtain the first PK column, to WAR the hasID table bug.
		if pk == nil && testIsPKHandleColumn(table, col) {
			pk = d.GetInt64()
		}
	}
	if pk == nil {
		pk = tidbRowID
		expected = append(expected, tidbRowID)
	}
	var pks []interface{}
	pks = append(pks, pk)
	expected = append(append(pks, expected...), makeInternalVersionValue(uint64(genCommitTS(base))), makeInternalDelmarkValue(delFlag))
	return datas, expected
}

func testFlashGenDatum(c *C, col *model.ColumnInfo, base int) (types.Datum, interface{}) {
	d, _ := testGenDatum(c, col, base)
	e, err := formatFlashData(d, col.FieldType)
	c.Assert(err, IsNil)
	return d, e
}

func testFlashGenInsertBinlog(c *C, table *model.TableInfo, r []types.Datum) []byte {
	colIDs := make([]int64, 0, len(r))
	row := make([]types.Datum, 0, len(r))
	var pk interface{}
	for _, col := range table.Columns {
		if pk == nil && testIsPKHandleColumn(table, col) {
			pk = r[col.Offset]
			continue
		}
		colIDs = append(colIDs, col.ID)
		row = append(row, r[col.Offset])
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	value, err := tablecodec.EncodeRow(sc, row, colIDs, nil, nil)
	c.Assert(err, IsNil)

	if pk == nil {
		pk = types.NewIntDatum(tidbRowID)
	}
	handleVal, _ := codec.EncodeValue(sc, nil, pk.(types.Datum))
	bin := append(handleVal, value...)
	return bin
}

func testFlashGenUpdateBinlog(c *C, table *model.TableInfo, oldData []types.Datum, newData []types.Datum) []byte {
	colIDs := make([]int64, 0, len(table.Columns))
	oldRow := make([]types.Datum, 0, len(oldData))
	newRow := make([]types.Datum, 0, len(newData))
	hasPK := false
	for _, col := range table.Columns {
		if testIsPKHandleColumn(table, col) {
			hasPK = true
		}
		colIDs = append(colIDs, col.ID)
		oldRow = append(oldRow, oldData[col.Offset])
		newRow = append(newRow, newData[col.Offset])
	}

	if !hasPK {
		colIDs = append(colIDs, implicitColID)
		oldRow = append(oldRow, types.NewIntDatum(tidbRowID))
		newRow = append(newRow, types.NewIntDatum(tidbRowID))
	}

	var bin []byte
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	newValue, err := tablecodec.EncodeRow(sc, newRow, colIDs, nil, nil)
	c.Assert(err, IsNil)
	oldValue, err := tablecodec.EncodeRow(sc, oldRow, colIDs, nil, nil)
	c.Assert(err, IsNil)
	bin = append(oldValue, newValue...)
	return bin
}

func testFlashGenDeleteBinlog(c *C, table *model.TableInfo, r []types.Datum) []byte {
	colIDs := make([]int64, 0, len(r))
	row := make([]types.Datum, 0, len(r))
	hasPK := false
	for _, col := range table.Columns {
		if testIsPKHandleColumn(table, col) {
			hasPK = true
		}
		colIDs = append(colIDs, col.ID)
		row = append(row, r[col.Offset])
	}

	if !hasPK {
		colIDs = append(colIDs, implicitColID)
		row = append(row, types.NewIntDatum(tidbRowID))
	}

	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	bin, err := tablecodec.EncodeRow(sc, row, colIDs, nil, nil)
	c.Assert(err, IsNil)
	return bin
}

func testGenFlashTranslator(c *C) *flashTranslator {
	translator, err := New("flash")
	c.Assert(err, IsNil)
	f, ok := translator.(*flashTranslator)
	c.Assert(ok, IsTrue)
	return f
}
