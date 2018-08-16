package drainer

import (
	"strconv"
	"testing"

	pb "github.com/pingcap/tipb/go-binlog"
)

const (
	binlogNum     = 100000
	maxSourceSize = 100
)

func BenchmarkMergeNormal5Source(b *testing.B) {
	merger := CreateMerger(5, binlogNum/5, normalStrategy)
	b.ResetTimer()
	ReadItem(merger.Output(), binlogNum-5)
}

func BenchmarkMergeHeap5Source(b *testing.B) {
	merger := CreateMerger(5, binlogNum/5, heapStrategy)
	b.ResetTimer()
	ReadItem(merger.Output(), binlogNum-5)
}

func BenchmarkMergeNormal10Source(b *testing.B) {
	merger := CreateMerger(10, binlogNum/10, normalStrategy)
	b.ResetTimer()
	ReadItem(merger.Output(), binlogNum-10)
}

func BenchmarkMergeHeap10Source(b *testing.B) {
	merger := CreateMerger(10, binlogNum/10, heapStrategy)
	b.ResetTimer()
	ReadItem(merger.Output(), binlogNum-10)
}

func BenchmarkMergeNormal50Source(b *testing.B) {
	merger := CreateMerger(50, binlogNum/50, normalStrategy)
	b.ResetTimer()
	ReadItem(merger.Output(), binlogNum-50)
}

func BenchmarkMergeHeap50Source(b *testing.B) {
	merger := CreateMerger(50, binlogNum/50, heapStrategy)
	b.ResetTimer()
	ReadItem(merger.Output(), binlogNum-50)
}

func ReadItem(itemCh chan MergeItem, total int) {
	num := 0
	for range itemCh {
		num++
		if num > total {
			return
		}
	}
}

func CreateMerger(sourceNum int, binlogNum int, strategy string) *Merger {
	sources := make([]MergeSource, sourceNum)
	for i := 0; i < sourceNum; i++ {
		source := MergeSource{
			ID:     strconv.Itoa(i),
			Source: make(chan MergeItem, binlogNum),
		}
		sources[i] = source
	}
	merger := NewMerger(0, strategy, sources...)

	for id := range sources {
		for j := 1; j <= binlogNum; j++ {
			binlog := new(pb.Binlog)
			binlog.CommitTs = int64(j*maxSourceSize + id)
			binlogItem := newBinlogItem(binlog, strconv.Itoa(id))
			sources[id].Source <- binlogItem
		}
	}

	return merger
}
