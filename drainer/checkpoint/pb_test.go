package checkpoint

import (
	"os"

	. "github.com/pingcap/check"
)

func (t *testCheckPointSuite) TestPb(c *C) {
	fileName := "test"
	notExistFileName := "test_not_exist"
	cfg := new(Config)
	cfg.CheckPointFile = fileName
	meta, err := newPb(cfg)
	c.Assert(err, IsNil)
	defer os.RemoveAll(fileName)

	testTs := int64(1)
	// save ts
	err = meta.Save(testTs)
	c.Assert(err, IsNil)
	// check ts
	ts := meta.Pos()
	c.Assert(ts, Equals, testTs)

	// check load ts
	err = meta.Load()
	c.Assert(err, IsNil)
	ts = meta.Pos()
	c.Assert(ts, Equals, testTs)

	// check not exist meta file
	cfg.CheckPointFile = notExistFileName
	err = meta.Load()
	c.Assert(err, IsNil)
}
