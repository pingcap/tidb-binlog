package checkpoint

import (
	"os"
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testCheckpointSuite{})

type testCheckpointSuite struct{}

func (s *testCheckpointSuite) TestCheckpoint(c *C) {
	fileNotExists := "file_not_exists"
	cp, err := newFileCheckpoint(fileNotExists)
	c.Assert(err, IsNil)

	pos, err := cp.Load()
	c.Assert(err, IsNil)
	c.Assert(pos.Filename, Equals, "")
	c.Assert(pos.Offset, Equals, int64(0))
	c.Assert(pos.Ts, Equals, int64(0))
	cp.Close()
	os.RemoveAll(fileNotExists)

	fileNotExists = "file_not_exists"
	cp, err = newFileCheckpoint(fileNotExists)
	c.Assert(err, IsNil)

	pos = &Position{
		Filename: "binlog-00001",
		Offset:   1,
		Ts:       1,
	}
	err = cp.Save(pos)
	c.Assert(err, IsNil)
	err = cp.Flush()
	c.Assert(err, IsNil)

	newPos, err := cp.Load()
	c.Logf("new pos %+v", newPos)
	c.Assert(err, IsNil)
	c.Assert(newPos, DeepEquals, pos)
	os.RemoveAll(fileNotExists)
}
