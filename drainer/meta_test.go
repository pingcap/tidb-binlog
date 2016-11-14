package drainer

import (
	"fmt"
	"os"

	. "github.com/pingcap/check"
)

func (t *testDrainerSuite) TestMeta(c *C) {
	fileName := "test"
	notExistFileName := "test_not_exist"
	meta := NewLocalMeta(fileName)
	defer os.RemoveAll(fileName)

	testTs := int64(1)
	// save ts
	err := meta.Save(testTs)
	c.Assert(err, IsNil)
	// check ts
	ts := meta.Pos()
	c.Assert(ts, Equals, testTs)
	// check load ts
	meta2 := NewLocalMeta(fileName)
	err = meta2.Load()
	c.Assert(err, IsNil)
	ts = meta2.Pos()
	c.Assert(ts, Equals, testTs)
	// check not exist meta file
	meta3 := NewLocalMeta(notExistFileName)
	err = meta3.Load()
	c.Assert(err, IsNil)
	// check String()
	c.Assert(fmt.Sprintf("%s", meta), Equals, "binlog test pos = 1")
}
