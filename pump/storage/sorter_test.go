package storage

import (
	"reflect"
	"testing"
	"time"

	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
)

func init() {
	log.SetLevel(log.LOG_LEVEL_ERROR)
}

func testSorter(t *testing.T, items []sortItem, expectMaxCommitTS []int64) {
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

	t.Logf("testSorter: %v, get maxCommitTS: %v", items, maxCommitTS)

	if expectMaxCommitTS != nil {
		if reflect.DeepEqual(maxCommitTS, expectMaxCommitTS) == false {
			t.Fatalf("want: %v, but get: %v", expectMaxCommitTS, maxCommitTS)
		}
	}

	if maxCommitTS[len(maxCommitTS)-1] != maxTS {
		t.Errorf("last maxCommitTS should be: %d, got: %d", maxTS, maxCommitTS[len(maxCommitTS)-1])
	}

}

func TestSorter(t *testing.T) {
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
	testSorter(t, items, []int64{2})

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
	testSorter(t, items, []int64{3, 10})

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
	testSorter(t, items, []int64{10})

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
	testSorter(t, items, []int64{10, 200})

	// test random data
	items = items[:0]
	for item := range newItemGenerator(5000) {
		items = append(items, item)
	}
	testSorter(t, items, nil)
}
