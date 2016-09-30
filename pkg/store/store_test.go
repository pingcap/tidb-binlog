package store

import (
	"fmt"
	"os"
	"testing"

	. "github.com/pingcap/check"
	pb "github.com/pingcap/tidb-binlog/proto"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testStoreSuite{})

type testStoreSuite struct {
	db      DB
	dirPath string
}

func (s *testStoreSuite) SetUpTest(c *C) {
	fmt.Println("xxx")
	tmpdir := os.TempDir()
	db, err := New(tmpdir)
	c.Assert(err, IsNil)
	s.dirPath = tmpdir
	s.db = db
}

func (s *testStoreSuite) TearDownSuite(c *C) {
	fmt.Println("yyy")
	s.db.Close()
	os.RemoveAll(s.dirPath)
}

func (s *testStoreSuite) TestDB(c *C) {
	s.MustPut(c, 1, &pb.Binlog{})
	s.MustPut(c, 2, &pb.Binlog{})
	s.MustPut(c, 3, &pb.Binlog{})
	s.MustPut(c, 4, &pb.Binlog{})
	s.MustPut(c, 5, &pb.Binlog{})
	s.MustPut(c, 6, &pb.Binlog{})

	_, err := s.db.Scan(1)
	c.Assert(err, IsNil)
}

func (s *testStoreSuite) MustPut(c *C, ts uint64, b *pb.Binlog) {
	err := s.db.Put(ts, b)
	c.Assert(err, IsNil)
}
