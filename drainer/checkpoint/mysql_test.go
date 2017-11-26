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
	cfg.Name = "checkpoint"
	nodeID := cfg.Name
	sp, err := newMysql(cfg)
	c.Assert(err, IsNil)

	testTs := int64(1)
	testPos := make(map[string]pb.Pos)
	testPos[nodeID] = pb.Pos{
		Suffix: 0,
		Offset: 5000,
	}
	err = sp.Save(testTs, testPos)
	c.Assert(err, IsNil)

	ts, poss := sp.Pos()
	c.Assert(ts, Equals, testTs)
	c.Assert(poss, HasLen, 1)
	c.Assert(poss[nodeID], DeepEquals, pb.Pos{Suffix: 0, Offset: 0})

	err = sp.Load()
	c.Assert(err, IsNil)
}
