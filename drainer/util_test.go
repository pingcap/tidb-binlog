package drainer

import (
	"errors"

	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
)

func (t *testDrainerSuite) TestIgnoreDDLError(c *C) {
	// test non-mysqltype error
	err := errors.New("test")
	ok := ignoreDDLError(err)
	c.Assert(ok, IsFalse)
	// test ignore error
	err1 := &mysql.MySQLError{
		Number:  1054,
		Message: "test",
	}
	ok = ignoreDDLError(err1)
	c.Assert(ok, IsTrue)
	// test non-ignore error
	err2 := &mysql.MySQLError{
		Number:  1052,
		Message: "test",
	}
	ok = ignoreDDLError(err2)
	c.Assert(ok, IsFalse)

}

func (t *testDrainerSuite) TestFormatIgnoreSchemas(c *C) {
	ignoreDBs := formatIgnoreSchemas("test,test1")
	ignoreList := make(map[string]struct{})
	ignoreList["test"] = struct{}{}
	ignoreList["test1"] = struct{}{}
	c.Assert(ignoreDBs, DeepEquals, ignoreList)
}

func (t *testDrainerSuite) TestFilterIgnoreSchema(c *C) {
	ignoreList := make(map[string]struct{})
	ignoreList["test"] = struct{}{}
	ignoreList["test1"] = struct{}{}

	c.Assert(filterIgnoreSchema(&model.DBInfo{Name: model.NewCIStr("test1")}, ignoreList), IsTrue)
	c.Assert(filterIgnoreSchema(&model.DBInfo{Name: model.NewCIStr("test2")}, ignoreList), IsFalse)
}
