package storage

import (
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
)

func init() {
	log.SetLevel(log.LOG_LEVEL_ERROR)
}

type SorterSuite struct{}

var _ = check.Suite(&SorterSuite{})

func testSorter(c *check.C, items []sortItem, expectMaxCommitTS []int64) {
	var maxCommitTS []int64
	var maxTS int64

	sorter := newSorter(func(item sortItem) {
		maxCommitTS = append(maxCommitTS, item.commit)
	})

	for _, item := range items {
		sorter.pushTSItem(item)
		if item.commit > maxTS {
			maxTS = item.commit
		}
	}

	time.Sleep(time.Millisecond * 50)
	sorter.close()

	c.Logf("testSorter: %v, get maxCommitTS: %v", items, maxCommitTS)

	if expectMaxCommitTS != nil {
		c.Assert(maxCommitTS, check.DeepEquals, expectMaxCommitTS)
	}

	c.Assert(maxTS, check.Equals, maxCommitTS[len(maxCommitTS)-1])
}

func (s *SorterSuite) TestSorter(c *check.C) {
	var items []sortItem

	items = []sortItem{
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
	for item := range newItemGenerator(5000) {
		items = append(items, item)
	}
	testSorter(c, items, nil)
}
