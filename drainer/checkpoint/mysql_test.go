package checkpoint

import (
	"os"
	"strconv"
	"testing"

	. "github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testCheckPointSuite{})

type testCheckPointSuite struct{}

func (*testCheckPointSuite) TestnewMysql(c *C) {
	cfg := new(Config)
	cfg.db = new(DBConfig)
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
	cfg.db.Host = host
	cfg.db.Port = port
	cfg.db.User = user
	cfg.db.Password = pass
	cfg.ClusterID = "checkpoint"
	cfg.Schema = "tidb_binlog"
	cfg.Table = "checkpoint"
	sp, err := newMysql(cfg)
	c.Assert(err, IsNil)

	testTs := int64(1)
	testPos := make(map[string]pb.Pos)
	testPos[cfg.ClusterID] = pb.Pos{
		Suffix: 0,
		Offset: 5000,
	}
	err = sp.Save(testTs, testPos)
	c.Assert(err, IsNil)

	ts, poss := sp.Pos()
	c.Assert(ts, Equals, testTs)
	c.Assert(poss, HasLen, 1)
	c.Assert(poss[cfg.ClusterID], DeepEquals, pb.Pos{Suffix: 0, Offset: 0})

	err = sp.Load()
	c.Assert(err, IsNil)
}
