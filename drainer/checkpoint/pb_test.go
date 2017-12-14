package checkpoint

import (
	"os"

	. "github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
)

func (t *testCheckPointSuite) TestPb(c *C) {
	fileName := "test"
	notExistFileName := "test_not_exist"
	cfg := new(Config)
	cfg.BinlogFileDir = fileName
	nodeID := fileName
	meta, err := newPb(cfg)
	c.Assert(err, IsNil)
	defer os.RemoveAll(fileName)

	testTs := int64(1)
	testPos := make(map[string]pb.Pos)
	testPos[nodeID] = pb.Pos{
		Suffix: 0,
		Offset: 10000,
	}
	// save ts
	err = meta.Save(testTs, testPos)
	c.Assert(err, IsNil)
	// check ts
	ts, poss := meta.Pos()
	c.Assert(ts, Equals, testTs)
	c.Assert(poss, HasLen, 1)
	c.Assert(poss[nodeID], DeepEquals, pb.Pos{
		Suffix: 0,
		Offset: 5000,
	})

	// check load ts
	err = meta.Load()
	c.Assert(err, IsNil)
	ts, poss = meta.Pos()
	c.Assert(ts, Equals, testTs)
	c.Assert(poss, HasLen, 1)
	c.Assert(poss[nodeID], DeepEquals, pb.Pos{
		Suffix: 0,
		Offset: 5000,
	})

	// check not exist meta file
	cfg.BinlogFileDir = notExistFileName
	err = meta.Load()
	c.Assert(err, IsNil)
}
