package savepoint

import (
	"os"
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSavepointSuite{})

type testSavepointSuite struct{}

func (s *testSavepointSuite) TestSavepoint(c *C) {
	fileNotExists := "file_not_exists"
	cp, err := newFileSavepoint(fileNotExists)
	c.Assert(err, IsNil)

	pos, err := cp.Load()
	c.Assert(err, IsNil)
	c.Assert(pos.Filename, Equals, "")
	c.Assert(pos.Offset, Equals, int64(0))
	c.Assert(pos.Ts, Equals, int64(0))
	cp.Close()
	os.RemoveAll(fileNotExists)

	fileNotExists = "file_not_exists"
	cp, err = newFileSavepoint(fileNotExists)
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
	c.Assert(cp.Pos(), DeepEquals, pos)

	newPos, err := cp.Load()
	c.Assert(err, IsNil)
	c.Assert(newPos, DeepEquals, pos)
	cp.Close()
	os.RemoveAll(fileNotExists)
}
