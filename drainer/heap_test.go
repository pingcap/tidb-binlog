package drainer

import (
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	. "github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testDrainerSuite{})

type testDrainerSuite struct{}

func (s *testDrainerSuite) TestHeap(c *C) {
	var wg sync.WaitGroup
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

	bh := newBinlogHeap(5)
	// test pop from empty heap
	c.Assert(bh.pop(), IsNil)
	// test push items into heap
	pctx := context.Background()
	ctx, cancel := context.WithCancel(pctx)
	go func() {
		wg.Add(1)
		defer wg.Done()
		for _, cs := range testCase {
			bh.push(ctx, newBinlogItem(cs, pb.Pos{}, "testnode"))
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

	//test push block and cancel push operator
	bh = newBinlogHeap(1)
	bh.push(ctx, newBinlogItem(testCase[0], pb.Pos{}, "testnode"))
	go func() {
		wg.Add(1)
		defer wg.Done()
		for _, cs := range testCase {
			bh.push(ctx, newBinlogItem(cs, pb.Pos{}, "testnode"))
		}
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()
	wg.Wait()
}
