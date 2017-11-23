package checkpoint

import (
    "testing"
   ."github.com/pingcap/check"
    pb "github.com/pingcap/tipb/go-binlog"  
)

func TestClient(t *testing.T) {
    TestingT(t)
}
var _ = Suite(&testExecutorSuite{})

type testExecutorSuite struct{}

func (*testExecutorSuite) TestnewMysqlSavePoint(c *C) {
    cfg := new(DBConfig)
    cfg.Host = "127.0.0.1"
    cfg.Port = 3306
    cfg.User = "root"
    cfg.Password = ""
    cfg.ClassId = "123"
    cfg.Schema = "test"
    cfg.Table = "t1"
    sp, err := newMysqlSavePoint(cfg)
    c.Assert(err, IsNil)

    testTs := int64(1)
    testPos := make(map[string]pb.Pos)
    testPos[cfg.ClassId] = pb.Pos{
        Suffix: 0,
        Offset: 5000,
    }
    err = sp.Save(testTs, testPos)
    c.Assert(err, IsNil)

    ts, poss := sp.Pos()
    c.Assert(ts, Equals, testTs)
    c.Assert(poss, HasLen, 1)
    c.Assert(poss[cfg.ClassId], DeepEquals, pb.Pos{Suffix:0,Offset:0})

    err = sp.Load()
    c.Assert(err, IsNil)

    c.Assert(sp.Check(), Equals, true)
}

