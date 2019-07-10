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

package storage

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
)

type SorterSuite struct{}

var _ = check.Suite(&SorterSuite{})

func testSorter(c *check.C, items []sortItem, expectMaxCommitTS []int64) {
	var maxCommitTS []int64
	var maxTS int64

	var lastGetSortItemTS int64
	sorter := newSorter(func(item sortItem) {
		maxCommitTS = append(maxCommitTS, item.commit)
		atomic.StoreInt64(&lastGetSortItemTS, item.commit)
	})

	for _, item := range items {
		sorter.pushTSItem(item)

		// we should never push item with commit ts less than lastGetSortItemTS, or something go wrong
		if item.tp == pb.BinlogType_Commit {
			c.Assert(item.commit, check.Greater, atomic.LoadInt64(&lastGetSortItemTS))
		}

		if item.commit > maxTS {
			maxTS = item.commit
		}
	}

	time.Sleep(time.Millisecond * 50)
	sorter.close()

	c.Logf("testSorter items: %v, get maxCommitTS: %v", items, maxCommitTS)
	c.Logf("items num: %d, get sort item num: %d", len(items), len(maxCommitTS))

	if expectMaxCommitTS != nil {
		c.Assert(maxCommitTS, check.DeepEquals, expectMaxCommitTS)
	}
	for i := 1; i < len(maxCommitTS); i++ {
		c.Assert(maxCommitTS[i], check.Greater, maxCommitTS[i-1])
	}
	c.Assert(maxTS, check.Equals, maxCommitTS[len(maxCommitTS)-1])
}

func (s *SorterSuite) TestSorter(c *check.C) {
	var items = []sortItem{
		{
			start: 1,
			tp:    pb.BinlogType_Prewrite,
		},
		{
			start:  1,
			commit: 2,
			tp:     pb.BinlogType_Commit,
		},
	}
	testSorter(c, items, []int64{2})

	items = []sortItem{
		{
			start: 1,
			tp:    pb.BinlogType_Prewrite,
		},
		{
			start: 2,
			tp:    pb.BinlogType_Prewrite,
		},
		{
			start:  2,
			commit: 3,
			tp:     pb.BinlogType_Commit,
		},
		{
			start:  1,
			commit: 10,
			tp:     pb.BinlogType_Commit,
		},
	}
	testSorter(c, items, []int64{3, 10})

	items = []sortItem{
		{
			start: 1,
			tp:    pb.BinlogType_Prewrite,
		},
		{
			start: 2,
			tp:    pb.BinlogType_Prewrite,
		},
		{
			start:  1,
			commit: 10,
			tp:     pb.BinlogType_Commit,
		},
		{
			start:  2,
			commit: 9,
			tp:     pb.BinlogType_Commit,
		},
	}
	testSorter(c, items, []int64{10})

	items = append(items, []sortItem{
		{
			start: 20,
			tp:    pb.BinlogType_Prewrite,
		},
		{
			start:  20,
			commit: 200,
			tp:     pb.BinlogType_Commit,
		},
	}...)
	testSorter(c, items, []int64{10, 200})

	// test random data
	items = items[:0]
	for item := range newItemGenerator(500, 10, 0) {
		items = append(items, item)
	}
	testSorter(c, items, nil)

	items = items[:0]
	for item := range newItemGenerator(5000, 1, 0) {
		items = append(items, item)
	}
	testSorter(c, items, nil)

	items = items[:0]
	for item := range newItemGenerator(500, 10, 10) {
		items = append(items, item)
	}
	testSorter(c, items, nil)

	items = items[:0]
	for item := range newItemGenerator(5000, 1, 10) {
		items = append(items, item)
	}
	testSorter(c, items, nil)
}
