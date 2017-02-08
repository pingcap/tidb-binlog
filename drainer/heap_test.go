package drainer

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testDrainerSuite{})

type testDrainerSuite struct{}

func (s *testDrainerSuite) TestHeap(c *C) {
	testCase := []*pb.Binlog{
		{CommitTs: 1},
		{CommitTs: 5},
		{CommitTs: 2},
		{CommitTs: 8},
		{CommitTs: 2},
	}
	excepted := []*pb.Binlog{
		{CommitTs: 1},
		{CommitTs: 2},
		{CommitTs: 2},
		{CommitTs: 5},
		{CommitTs: 8},
	}

	bh := newBinlogHeap()
	// test pop from empty heap
	c.Assert(bh.pop(), IsNil)
	// test push items into heap
	go func() {
		for _, cs := range testCase {
			bh.push(newBinlogItem(cs, pb.Pos{}, "testnode"))
		}
	}()
	// test pop items from heap
	var index int
	for index < len(excepted) {
		item := bh.pop()
		if item == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		c.Assert(item.binlog.CommitTs, Equals, excepted[index].CommitTs)
		index++
	}
}
