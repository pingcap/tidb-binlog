package diff

import (
	"database/sql"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testDBSuite{})

var _ = Suite(&testEqualJSON{})

type testEqualJSON struct{}

type testDBSuite struct {
	user      string
	pass      string
	prot      string
	addr      string
	dbname    string
	dsn       string
	netAddr   string
	available bool
}

var (
	tDate      = time.Date(2012, 6, 14, 0, 0, 0, 0, time.UTC)
	sDate      = "2012-06-14"
	tDateTime  = time.Date(2011, 11, 20, 21, 27, 37, 0, time.UTC)
	sDateTime  = "2011-11-20 21:27:37"
	tDate0     = time.Time{}
	sDate0     = "0000-00-00"
	sDateTime0 = "0000-00-00 00:00:00"
)

func (s *testDBSuite) SetUpSuite(c *C) {
	// get environment variables
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}
	s.user = env("MYSQL_TEST_USER", "root")
	s.pass = env("MYSQL_TEST_PASS", "")
	s.prot = env("MYSQL_TEST_PROT", "tcp")
	s.addr = env("MYSQL_TEST_ADDR", "localhost:4000")
	s.dbname = env("MYSQL_TEST_DBNAME", "test")
	s.netAddr = fmt.Sprintf("%s(%s)", s.prot, s.addr)

	s.dsn = fmt.Sprintf("%s:%s@%s/%s?timeout=30s", s.user, s.pass, s.netAddr, s.dbname)
	conn, err := net.Dial(s.prot, s.addr)
	if err == nil {
		s.available = true
		conn.Close()
	}
}

func (s *testDBSuite) TestDiff(c *C) {
	if !s.available {
		c.Skip("no mysql available")
	}

	db, err := sql.Open("mysql", s.dsn)
	c.Assert(err, IsNil)

	_, err = db.Query("create table tidb_binlog_diff_test(id int, jdoc json);")
	defer db.Query("drop table tidb_binlog_diff_test;")
	c.Assert(err, IsNil)
	_, err = db.Query(
		`INSERT INTO tidb_binlog_diff_test(jdoc) VALUES('{"key1": "value1", "key2": "value2"}');
	`)
	c.Assert(err, IsNil)

	df := New(db, db)
	eq, err := df.Equal()
	c.Assert(err, IsNil)
	c.Assert(eq, IsTrue)
}

func (s *testEqualJSON) TestAll(c *C) {
	var d1 string
	var d2 string
	var match bool

	d1 = `{"key1":"value1","key2":"value2"}`
	d2 = `{"key1":"value1","key2":"value2"}`
	match = equalJSON([]byte(d1), []byte(d2))
	c.Assert(match, IsTrue)

	// add space
	d1 = `{"key1":"value1", "key2":"value2"}`
	match = equalJSON([]byte(d1), []byte(d2))
	c.Assert(match, IsTrue)

	// reorder key
	d1 = `{"key2":"value2","key1":"value1"}`
	match = equalJSON([]byte(d1), []byte(d2))
	c.Assert(match, IsTrue)

	//test some empty
	match = equalJSON([]byte(""), []byte("{}"))
	c.Assert(match, IsFalse)

	match = equalJSON([]byte("{}"), []byte(""))
	c.Assert(match, IsFalse)

	match = equalJSON([]byte(""), []byte(""))
	c.Assert(match, IsTrue)
}
