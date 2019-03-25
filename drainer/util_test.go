package drainer

import (
	"errors"

	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/sql"
)

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
