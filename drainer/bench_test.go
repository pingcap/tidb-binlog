// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package drainer

import (
	"strconv"
	"testing"

	pb "github.com/pingcap/tipb/go-binlog"
)

const (
	maxSourceSize = 100
)

func BenchmarkMergeNormal5Source(b *testing.B) {
	merger := CreateMerger(5, b.N, normalStrategy)
	b.ResetTimer()
	ReadItem(merger.Output(), b.N)
}

func BenchmarkMergeHeap5Source(b *testing.B) {
	merger := CreateMerger(5, b.N, heapStrategy)
	b.ResetTimer()
	ReadItem(merger.Output(), b.N)
}

func BenchmarkMergeNormal10Source(b *testing.B) {
	merger := CreateMerger(10, b.N, normalStrategy)
	b.ResetTimer()
	ReadItem(merger.Output(), b.N)
}

func BenchmarkMergeHeap10Source(b *testing.B) {
	merger := CreateMerger(10, b.N, heapStrategy)
	b.ResetTimer()
	ReadItem(merger.Output(), b.N)
}

func BenchmarkMergeNormal50Source(b *testing.B) {
	merger := CreateMerger(50, b.N, normalStrategy)
	b.ResetTimer()
	ReadItem(merger.Output(), b.N)
}

func BenchmarkMergeHeap50Source(b *testing.B) {
	merger := CreateMerger(50, b.N, heapStrategy)
	b.ResetTimer()
	ReadItem(merger.Output(), b.N)
}

func ReadItem(itemCh chan MergeItem, total int) {
	num := 0
	for range itemCh {
		num++
		if num > total {
			break
		}
	}
}

func CreateMerger(sourceNum int, binlogNum int, strategy string) *Merger {
	sources := make([]MergeSource, sourceNum)
	for i := 0; i < sourceNum; i++ {
		source := MergeSource{
			ID:     strconv.Itoa(i),
			Source: make(chan MergeItem, binlogNum/sourceNum+sourceNum),
		}
		sources[i] = source
	}
	merger := NewMerger(0, strategy, sources...)

	for id := range sources {
		for j := 1; j <= binlogNum/sourceNum+sourceNum; j++ {
			binlog := new(pb.Binlog)
			binlog.CommitTs = int64(j*maxSourceSize + id)
			binlogItem := newBinlogItem(binlog, strconv.Itoa(id))
			sources[id].Source <- binlogItem
		}
	}

	return merger
}
