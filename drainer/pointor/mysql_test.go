package pointor

import (
//    "database/sql"
//    "database/sql/driver"
    "testing"
    "fmt"
//    "github.com/juju/errors"
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

    sp, err := newMysqlPoint(cfg)
    c.Assert(err, IsNil) 
    testTs := int64(1)
    testPos := make(map[string]pb.Pos)
    testPos["test"] = pb.Pos{
        Suffix: 0,
        Offset: 5000,
    }
    fmt.Println(sp)
    err = sp.Save(testTs, testPos)
    c.Assert(err, IsNil)
    
    fmt.Println("111111111111111")
    ts, poss := sp.Pos()
    fmt.Println("222")

    c.Assert(ts, Equals, testTs)
    fmt.Println("333")
    c.Assert(poss, HasLen, 1)
    fmt.Println("444")
    c.Assert(poss["test"], DeepEquals, pb.Pos{Suffix:0,Offset:0})
    fmt.Println("555")
}

