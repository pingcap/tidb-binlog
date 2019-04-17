package translator

import (
	"fmt"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	parsermysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTranslatorSuite{})

type testTranslatorSuite struct{}

// test the already implemented translator, register and unregister function
func (t *testTranslatorSuite) TestRegisterAndUnregister(c *C) {
	hasTranslaters := []string{"mysql"}
	for _, name := range hasTranslaters {
		_, err := New(name)
		c.Assert(err, IsNil)
	}

	testName := "myTest"

	Register(testName, &mysqlTranslator{})
	_, err := New(testName)
	c.Assert(err, IsNil)

	Unregister(testName)
	_, err = New(testName)
	if err == nil {
		c.Fatalf("%s should not be existed", testName)
	}
}

// test all already implemented translator's translation function
func (t *testTranslatorSuite) TestTranslater(c *C) {
	s, err := New("mysql")
	c.Assert(err, IsNil)
	testGenInsertSQLs(c, s, true)
	testGenInsertSQLs(c, s, false)
	testGenUpdateSQLs(c, s)
	testGenDeleteSQLs(c, s)
	testGenDDLSQL(c, s)
}

func testGenInsertSQLs(c *C, s SQLTranslator, safeMode bool) {
	s.SetConfig(safeMode, parsermysql.ModeStrictTransTables|parsermysql.ModeNoEngineSubstitution)
	schema := "t"
	tables := []*model.TableInfo{testGenTable("normal"), testGenTable("hasPK"), testGenTable("hasID")}
	exceptedKeys := []int{3, 2, 1}
	for i, table := range tables {
		rowDatas, expected, expectedKeys := testGenRowData(c, table.Columns, 1)
		binlog := testGenInsertBinlog(c, table, rowDatas)
		sqls, keys, vals, err := s.GenInsertSQLs(schema, table, [][]byte{binlog}, 0)
		c.Assert(fmt.Sprintf("%v", keys[0]), Equals, fmt.Sprintf("[%s]", strings.Join(expectedKeys[:exceptedKeys[i]], ",")))
		c.Assert(err, IsNil)
		c.Assert(len(vals[0]), Equals, 3)
		if safeMode {
			c.Assert(sqls[0], Equals, "replace into `t`.`account` (`ID`,`NAME`,`SEX`) values (?,?,?);")
		} else {
			c.Assert(sqls[0], Equals, "insert into `t`.`account` (`ID`,`NAME`,`SEX`) values (?,?,?);")
		}
		for index := range vals[0] {
			c.Assert(vals[0][index], DeepEquals, expected[index])
		}
	}

	rowDatas, _, _ := testGenRowData(c, tables[0].Columns, 1)
	binlog := testGenInsertBinlog(c, tables[0], rowDatas)
	_, _, _, err := s.GenInsertSQLs(schema, tables[0], [][]byte{binlog[6:]}, 0)
	c.Assert(err, NotNil)
}

func testGenUpdateSQLs(c *C, s SQLTranslator) {
	schema := "t"
	tables := []*model.TableInfo{testGenTable("normal"), testGenTable("hasPK"), testGenTable("hasID")}
	exceptedSQL := []string{
		"update `t`.`account` set `ID` = ?, `NAME` = ?, `SEX` = ? where `ID` = ? and `NAME` = ? and `SEX` = ? limit 1;",
		"update `t`.`account` set `ID` = ?, `NAME` = ?, `SEX` = ? where `ID` = ? and `NAME` = ? limit 1;",
		"update `t`.`account` set `ID` = ?, `NAME` = ?, `SEX` = ? where `ID` = ? limit 1;",
	}
	exceptedNum := []int{6, 5, 4}
	exceptedKeys := []int{3, 2, 1}
	for index, t := range tables {
		oldRowDatas, whereExpected, whereKeys := testGenRowData(c, t.Columns, 1)
		newRowDatas, changedExpected, changedKeys := testGenRowData(c, t.Columns, 2)
		binlog := testGenUpdateBinlog(c, t, oldRowDatas, newRowDatas)
		sqls, keys, vals, _, err := s.GenUpdateSQLs(schema, t, [][]byte{binlog}, 0)
		c.Assert(err, IsNil)
		c.Assert(fmt.Sprintf("%v", keys[0]), Equals, fmt.Sprintf("%v", []string{strings.Join(changedKeys[:exceptedKeys[index]], ","), strings.Join(whereKeys[:exceptedKeys[index]], ",")}))
		c.Assert(len(vals[0]), Equals, exceptedNum[index])
		c.Assert(sqls[0], Equals, exceptedSQL[index])
		for index := range vals[0] {
			if index < 3 {
				c.Assert(vals[0][index], DeepEquals, changedExpected[index])
				continue
			}
			c.Assert(vals[0][index], DeepEquals, whereExpected[index%3])
		}
	}

	rowDatas, _, _ := testGenRowData(c, tables[0].Columns, 1)
	binlog := testGenUpdateBinlog(c, tables[0], rowDatas, rowDatas)
	_, _, _, _, err := s.GenUpdateSQLs(schema, tables[0], [][]byte{binlog[6:]}, 0)
	c.Assert(err, NotNil)
}

func testGenDeleteSQLs(c *C, s SQLTranslator) {
	schema := "t"
	tables := []*model.TableInfo{testGenTable("normal"), testGenTable("hasPK")}
	exceptedSQL := []string{
		"delete from `t`.`account` where `ID` = ? and `NAME` = ? and `SEX` = ? limit 1;",
		"delete from `t`.`account` where `ID` = ? and `NAME` = ? limit 1;",
	}
	exceptedNum := []int{3, 2}
	exceptedKeys := []int{3, 2}
	for index, t := range tables {
		rowDatas, expected, expectedKeys := testGenRowData(c, t.Columns, 1)
		binlog := testGenDeleteBinlog(c, t, rowDatas)
		sqls, keys, vals, err := s.GenDeleteSQLs(schema, t, [][]byte{binlog}, 0)
		c.Assert(fmt.Sprintf("%v", keys[0]), Equals, fmt.Sprintf("[%s]", strings.Join(expectedKeys[:exceptedKeys[index]], ",")))
		c.Assert(err, IsNil)
		c.Assert(len(vals[0]), Equals, exceptedNum[index])
		c.Assert(sqls[0], Equals, exceptedSQL[index])
		for index := range vals[0] {
			c.Assert(vals[0][index], DeepEquals, expected[index])
		}
	}

	rowDatas, _, _ := testGenRowData(c, tables[0].Columns, 1)
	binlog := testGenDeleteBinlog(c, tables[0], rowDatas)
	_, _, _, err := s.GenDeleteSQLs(schema, tables[0], [][]byte{binlog[6:]}, 0)
	c.Assert(err, NotNil)
}

func testGenDDLSQL(c *C, s SQLTranslator) {
	sql, err := s.GenDDLSQL("create database t", "t", 0)
	c.Assert(err, IsNil)
	c.Assert(sql, Equals, "create database t;")

	sql, err = s.GenDDLSQL("drop table t", "t", 0)
	c.Assert(err, IsNil)
	c.Assert(sql, Equals, "use `t`; drop table t;")
}

func testGenRowData(c *C, cols []*model.ColumnInfo, base int) ([]types.Datum, []interface{}, []string) {
	datas := make([]types.Datum, len(cols))
	excepted := make([]interface{}, len(cols))
	keys := make([]string, len(cols))
	for index, col := range cols {
		d, e := testGenDatum(c, col, base)
		datas[index] = d
		excepted[index] = e
		keys[index] = fmt.Sprintf("(%s: %v)", col.Name, e)

	}
	return datas, excepted, keys
}
