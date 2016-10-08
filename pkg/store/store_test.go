package store

import (
	"os"
	"testing"
	"time"

	. "github.com/pingcap/check"
	pb "github.com/pingcap/tidb-binlog/proto"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testStoreSuite{})

type testStoreSuite struct {
	dirPath string
}

func (s *testStoreSuite) SetUpTest(c *C) {
	tmpdir := os.TempDir()
	s.dirPath = tmpdir
}

func (s *testStoreSuite) TearDownSuite(c *C) {
	os.RemoveAll(s.dirPath)
}

func (s *testStoreSuite) TestDB(c *C) {
	db, err := New(s.dirPath, 0)
	c.Assert(err, IsNil)
	defer db.Close()

	for i := uint64(1); i <= 100; i++ {
		mustPut(db, c, i, &pb.Binlog{})
	}

	iter, err := db.Scan(1)
	c.Assert(err, IsNil)

	key := 0
	for ; iter.Valid(); iter.Next() {
		key++
		ts, err := iter.Key()
		c.Assert(err, IsNil)
		c.Assert(ts, Equals, key)

		_, err = iter.Value()
		c.Assert(err, IsNil)
	}
}

func (s *testStoreSuite) TestScanAfterPut(c *C) {
	db, err := New(s.dirPath, 3*time.Second)
	c.Assert(err, IsNil)
	defer db.Close()

	for i := uint64(1); i <= 50; i++ {
		mustPut(db, c, i, &pb.Binlog{})
	}

	iter, err := db.Scan(1)
	c.Assert(err, IsNil)
	// Scan immediately after Put, iter should get nothing
	count := 0
	for ; iter.Valid(); iter.Next() {
		count++
	}
	c.Assert(count, Equals, 0)

	// After sleep 3s, data should be visible to Scan
	time.Sleep(3 * time.Second)
	iter, err = db.Scan(1)
	key := 0
	for ; iter.Valid(); iter.Next() {
		key++
		ts, err := iter.Key()
		c.Assert(err, IsNil)
		c.Assert(ts, Equals, key)
	}
}

func mustPut(db DB, c *C, ts uint64, b *pb.Binlog) {
	err := db.Put(ts, b)
	c.Assert(err, IsNil)
}
