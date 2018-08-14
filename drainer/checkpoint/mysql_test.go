package checkpoint

import (
	"os"
	"strconv"
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testCheckPointSuite{})

type testCheckPointSuite struct{}

func (*testCheckPointSuite) TestnewMysql(c *C) {
	cfg := new(Config)
	cfg.Db = new(DBConfig)
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if port == 0 {
		port = 3306
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}
	pass := os.Getenv("MYSQL_PSWD")
	cfg.Db.Host = host
	cfg.Db.Port = port
	cfg.Db.User = user
	cfg.Db.Password = pass
	cfg.ClusterID = 123
	cfg.Schema = "tidb_binlog"
	cfg.Table = "checkpoint"
	sp, err := newMysql("mysql", cfg)
	c.Assert(err, IsNil)

	testTs := int64(1)
	err = sp.Save(testTs)
	c.Assert(err, IsNil)

	ts := sp.TS()
	c.Assert(ts, Equals, testTs)

	sp1, ero := newMysql("mysql", cfg)
	c.Assert(ero, IsNil)
	err = sp1.Load()
	c.Assert(err, IsNil)
	sp2 := sp1.(*MysqlCheckPoint)
	c.Assert(sp2.CommitTS, Equals, ts)
}
