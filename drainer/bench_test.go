package drainer

import (
	"strconv"
	"testing"

	pb "github.com/pingcap/tipb/go-binlog"
)

const (
	binlogNum = 10000
)

func BenchmarkMergeNormal10Source(b *testing.B) {
	merger := CreateMerger(10, binlogNum, "normal")
	b.ResetTimer()
	ReadItem(merger.Output(), 10*(binlogNum-1))
}

func BenchmarkMergeHeap10Source(b *testing.B) {
	merger := CreateMerger(10, binlogNum, "heap")
	b.ResetTimer()
	ReadItem(merger.Output(), 10*(binlogNum-1))
}

func BenchmarkMergeNormal50Source(b *testing.B) {
	merger := CreateMerger(50, binlogNum, "normal")
	b.ResetTimer()
	ReadItem(merger.Output(), 50*(binlogNum-1))
}

func BenchmarkMergeHeap50Source(b *testing.B) {
	merger := CreateMerger(50, binlogNum, "heap")
	b.ResetTimer()
	ReadItem(merger.Output(), 50*(binlogNum-1))
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
			Source: make(chan MergeItem, 10),
		}
		sources[i] = source
	}
	merger := NewMerger(0, strategy, sources...)

	for id := range sources {
		go func(id int) {
			for j := 1; j <= binlogNum; j++ {
				binlog := new(pb.Binlog)
				binlog.CommitTs = int64(j*100 + id)
				binlogItem := newBinlogItem(binlog, strconv.Itoa(id))
				sources[id].Source <- binlogItem
			}
		}(id)
	}

	return merger
}
