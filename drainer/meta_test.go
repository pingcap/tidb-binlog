package drainer

import (
	"os"

	. "github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
)

func (t *testDrainerSuite) TestMeta(c *C) {
	fileName := "test"
	notExistFileName := "test_not_exist"
	meta := NewLocalMeta(fileName)
	defer os.RemoveAll(fileName)

	testTs := int64(1)
	testPos := make(map[string]pb.Pos)
	testPos["test"] = pb.Pos{
		Suffix: 0,
		Offset: 10000,
	}
	// save ts
	err := meta.Save(testTs, testPos)
	c.Assert(err, IsNil)
	// check ts
	ts, poss := meta.Pos()
	c.Assert(ts, Equals, testTs)
	c.Assert(poss, HasLen, 1)
	c.Assert(poss["test"], DeepEquals, pb.Pos{
		Suffix: 0,
		Offset: 5000,
	})
	// check load ts
	meta2 := NewLocalMeta(fileName)
	err = meta2.Load()
	c.Assert(err, IsNil)
	ts, poss = meta2.Pos()
	c.Assert(ts, Equals, testTs)
	c.Assert(poss, HasLen, 1)
	c.Assert(poss["test"], DeepEquals, pb.Pos{
		Suffix: 0,
		Offset: 5000,
	})
	// check not exist meta file
	meta3 := NewLocalMeta(notExistFileName)
	err = meta3.Load()
	c.Assert(err, IsNil)
}
