package drainer

import (
	"errors"

	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/sql"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tipb/go-binlog"
)

func (t *testDrainerSuite) TestComparePos(c *C) {
	c.Assert(ComparePos(binlog.Pos{Suffix: 2, Offset: 3}, binlog.Pos{Suffix: 3, Offset: 4}), Equals, -1)
	c.Assert(ComparePos(binlog.Pos{Suffix: 4, Offset: 3}, binlog.Pos{Suffix: 3, Offset: 4}), Equals, 1)
	c.Assert(ComparePos(binlog.Pos{Suffix: 3, Offset: 2}, binlog.Pos{Suffix: 3, Offset: 4}), Equals, -1)
	c.Assert(ComparePos(binlog.Pos{Suffix: 3, Offset: 5}, binlog.Pos{Suffix: 3, Offset: 4}), Equals, 1)
	c.Assert(ComparePos(binlog.Pos{Suffix: 3, Offset: 4}, binlog.Pos{Suffix: 3, Offset: 4}), Equals, 0)
}

func (t *testDrainerSuite) TestPosToFloat(c *C) {
	pos := binlog.Pos{
		Suffix: 4,
		Offset: 3721,
	}
	f := posToFloat(&pos)
	c.Assert(f, Equals, 4*1000*1000*1000+3721)
}

func (t *testDrainerSuite) TestIgnoreDDLError(c *C) {
	// test non-mysqltype error
	err := errors.New("test")
	ok := sql.IgnoreDDLError(err)
	c.Assert(ok, IsFalse)
	// test ignore error
	err1 := &mysql.MySQLError{
		Number:  1054,
		Message: "test",
	}
	ok = sql.IgnoreDDLError(err1)
	c.Assert(ok, IsTrue)
	// test non-ignore error
	err2 := &mysql.MySQLError{
		Number:  1052,
		Message: "test",
	}
	ok = sql.IgnoreDDLError(err2)
	c.Assert(ok, IsFalse)

}

func (t *testDrainerSuite) TestFormatIgnoreSchemas(c *C) {
	ignoreDBs := formatIgnoreSchemas("test1,test2")
	ignoreList := make(map[string]struct{})
	ignoreList["test1"] = struct{}{}
	ignoreList["test2"] = struct{}{}
	c.Assert(ignoreDBs, DeepEquals, ignoreList)
}

func (t *testDrainerSuite) TestFilterIgnoreSchema(c *C) {
	ignoreList := make(map[string]struct{})
	ignoreList["test"] = struct{}{}
	ignoreList["test1"] = struct{}{}

	c.Assert(filterIgnoreSchema(&model.DBInfo{Name: model.NewCIStr("test1")}, ignoreList), IsTrue)
	c.Assert(filterIgnoreSchema(&model.DBInfo{Name: model.NewCIStr("test2")}, ignoreList), IsFalse)
}
