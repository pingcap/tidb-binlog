package sql

import (
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) { TestingT(t) }

type quoteSuite struct{}

var _ = Suite(&quoteSuite{})

func (s *quoteSuite) TestQuoteSchema(c *C) {
	c.Assert(QuoteSchema("music", "subjects"), Equals, "`music`.`subjects`")
	c.Assert(QuoteSchema("wEi`rd", "Na`me"), Equals, "`wEi``rd`.`Na``me`")
}

type parseCHAddrSuite struct{}

var _ = Suite(&parseCHAddrSuite{})

func (s *parseCHAddrSuite) TestShouldRetErrForInvalidAddr(c *C) {
	_, err := ParseCHAddr("localhost:8080,localhost:8X8X")
	c.Assert(err, NotNil)
	_, err = ParseCHAddr("asdfasdf")
	c.Assert(err, NotNil)
}

func (s *parseCHAddrSuite) TestShouldRetHostAndPort(c *C) {
	addrs, err := ParseCHAddr("localhost:8080,test2:8081")
	c.Assert(err, IsNil)
	c.Assert(addrs, HasLen, 2)
	c.Assert(addrs[0].Host, Equals, "localhost")
	c.Assert(addrs[0].Port, Equals, 8080)
	c.Assert(addrs[1].Host, Equals, "test2")
	c.Assert(addrs[1].Port, Equals, 8081)
}

type composeCHDSNSuite struct{}

var _ = Suite(&composeCHDSNSuite{})

func (s *composeCHDSNSuite) TestShouldIncludeAllInfo(c *C) {
	dbDSN := composeCHDSN("test_node1", 7979, "root", "secret", "test", 1024)
	c.Assert(dbDSN, Equals, "tcp://test_node1:7979?username=root&password=secret&database=test&block_size=1024&")
	dbDSN = composeCHDSN("test", 7979, "root", "", "test", -1)
	c.Assert(dbDSN, Equals, "tcp://test:7979?username=root&database=test&")
}

type SQLErrSuite struct{}

var _ = Suite(&SQLErrSuite{})

func (s *SQLErrSuite) TestGetSQLErrCode(c *C) {
	_, ok := GetSQLErrCode(errors.New("test"))
	c.Assert(ok, IsFalse)
	_, ok = GetSQLErrCode(&mysql.MySQLError{Number: 1146})
	c.Assert(ok, IsTrue)
}

func (s *SQLErrSuite) TestIgnoreDDLError(c *C) {
	c.Assert(IgnoreDDLError(&mysql.MySQLError{Number: 1146}), IsTrue)
	c.Assert(IgnoreDDLError(&mysql.MySQLError{Number: 1032}), IsFalse)
}
