package drainer

import (
	"container/heap"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/model"
	pb "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
)

var (
	pushRetryTime = 10 * time.Millisecond
)

type binlogData struct {
	tp            pb.BinlogType
	startTs       int64
	commitTs      int64
	prewriteKey   []byte
	prewriteValue *pb.PrewriteValue
	ddlQuery      []byte
	ddlJobID      int64
}

// SetMumations sets the prewriteValue.Mutations
func (b *binlogData) setMumations(mumations []pb.TableMutation) {
	b.prewriteValue.Mutations = mumations
}

type binlogItem struct {
	binlog *binlogData
	pos    pb.Pos
	nodeID string
	job    *model.Job
}

func newBinlogItem(b *pb.Binlog, p pb.Pos, nodeID string) *binlogItem {
	preWriteValue := b.GetPrewriteValue()
	preWrite := &pb.PrewriteValue{}
	err := preWrite.Unmarshal(preWriteValue)
	if err != nil {
		log.Errorf("prewrite %s unmarshal error %v", preWriteValue, err)
		return nil
	}
	newBinlog := &binlogData{
		tp:            b.GetTp(),
		startTs:       b.GetStartTs(),
		commitTs:      b.GetCommitTs(),
		prewriteKey:   b.GetPrewriteKey(),
		prewriteValue: preWrite,
		ddlQuery:      b.GetDdlQuery(),
		ddlJobID:      b.GetDdlJobId(),
	}

	return &binlogItem{
		binlog: newBinlog,
		pos:    p,
		nodeID: nodeID,
	}
}

func (b *binlogItem) SetJob(job *model.Job) {
	b.job = job
}

type binlogItems []*binlogItem

func (b binlogItems) Len() int           { return len(b) }
func (b binlogItems) Less(i, j int) bool { return b[i].binlog.commitTs < b[j].binlog.commitTs }
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
	bh   heap.Interface
	size int
}

func newBinlogHeap(size int) *binlogHeap {
	return &binlogHeap{
		bh:   &binlogItems{},
		size: size,
	}
}

func (b *binlogHeap) push(ctx context.Context, item *binlogItem) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			b.Lock()
			if b.bh.Len() == b.size {
				b.Unlock()
				time.Sleep(pushRetryTime)
				continue
			}
			heap.Push(b.bh, item)
			b.Unlock()
			return
		}
	}
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
