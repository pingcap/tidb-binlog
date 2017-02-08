package heap

import (
	"container/heap"
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testHeapSuite{})

type testHeapSuite struct{}

type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (s *testHeapSuite) TestHeap(c *C) {
	testCase := []int{5, 1, 2, 8, 2}
	excepted := []int{1, 2, 2, 5, 8}
	h := &IntHeap{}
	heap.Init(h)

	sh := NewSafeHeap(h)
	// test pop from empty heap
	c.Assert(sh.Pop(), IsNil)
	// test push items into heap
	for _, cs := range testCase {
		sh.Push(cs)
	}
	// test pop items from heap
	for _, cs := range excepted {
		c.Assert(cs, Equals, sh.Pop())
	}
}
