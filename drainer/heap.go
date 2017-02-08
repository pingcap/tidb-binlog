package drainer

import (
	"container/heap"
	"sync"

	"github.com/pingcap/tidb/model"
	pb "github.com/pingcap/tipb/go-binlog"
)

type binlogItem struct {
	binlog *pb.Binlog
	pos    pb.Pos
	nodeID string
	job    *model.Job
}

func newBinlogItem(b *pb.Binlog, p pb.Pos, nodeID string) *binlogItem {
	return &binlogItem{
		binlog: b,
		pos:    p,
		nodeID: nodeID,
	}
}

func (b *binlogItem) SetJob(job *model.Job) {
	b.job = job
}

type binlogItems []*binlogItem

func (b binlogItems) Len() int           { return len(b) }
func (b binlogItems) Less(i, j int) bool { return b[i].binlog.CommitTs < b[j].binlog.CommitTs }
func (b binlogItems) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

// Push implements heap.Interface's Push function
func (b *binlogItems) Push(x interface{}) {
	*b = append(*b, x.(*binlogItem))
}

// Pop implements heap.Interface's Pop function
func (b *binlogItems) Pop() interface{} {
	old := *b
	n := len(old)
	x := old[n-1]
	*b = old[0 : n-1]
	return x
}

type binlogHeap struct {
	sync.Mutex
	bh heap.Interface
}

func newBinlogHeap() *binlogHeap {
	return &binlogHeap{
		bh: &binlogItems{},
	}
}

func (b *binlogHeap) push(item *binlogItem) {
	b.Lock()
	heap.Push(b.bh, item)
	b.Unlock()
}

func (b *binlogHeap) pop() *binlogItem {
	b.Lock()
	if b.bh.Len() == 0 {
		b.Unlock()
		return nil
	}

	item := heap.Pop(b.bh)
	b.Unlock()
	return item.(*binlogItem)
}
