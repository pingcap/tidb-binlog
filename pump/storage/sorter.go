package storage

import (
	"container/list"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/pingcap/tipb/go-binlog"
)

// a test helper, generate n pair item
func newItemGenerator(txnNum int32) <-chan sortItem {
	items := make(chan sortItem)
	oracle := newOracle()

	threadNum := 10
	var maxLatency int64 = 10

	var wg sync.WaitGroup

	for i := 0; i < threadNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if atomic.AddInt32(&txnNum, -1) < 0 {
					return
				}

				start := oracle.getTS()
				items <- sortItem{
					start: start,
					tp:    pb.BinlogType_Prewrite,
				}

				time.Sleep(time.Millisecond * time.Duration(rand.Int63()%maxLatency))

				commit := oracle.getTS()
				items <- sortItem{
					start:  start,
					commit: commit,
					tp:     pb.BinlogType_Commit,
				}

				time.Sleep(time.Millisecond * time.Duration(rand.Int63()%maxLatency))
			}

		}()
	}

	go func() {
		wg.Wait()
		close(items)
	}()

	return items
}

/// sorter
type sortItem struct {
	start  int64
	commit int64
	tp     pb.BinlogType
}

type sorter struct {
	maxTSItemCB func(item sortItem)
	waitStarts  map[int64]struct{}

	lock     sync.Mutex
	cond     *sync.Cond
	items    *list.List
	isClosed int32
}

func newSorter(fn func(item sortItem)) *sorter {
	sorter := &sorter{
		maxTSItemCB: fn,
		items:       list.New(),
		waitStarts:  make(map[int64]struct{}),
	}

	sorter.cond = sync.NewCond(&sorter.lock)
	go sorter.run()

	return sorter
}

func (s *sorter) pushTSItem(item sortItem) {
	s.lock.Lock()

	if item.tp == pb.BinlogType_Prewrite {
		s.waitStarts[item.start] = struct{}{}
	} else {
		delete(s.waitStarts, item.start)
		s.cond.Signal()
	}

	s.items.PushBack(item)
	if s.items.Len() == 1 {
		s.cond.Signal()
	}
	s.lock.Unlock()
}

func (s *sorter) run() {
	var maxTSItem sortItem
	for {
		s.cond.L.Lock()
		for s.items.Len() == 0 {
			if atomic.LoadInt32(&s.isClosed) == 1 {
				return
			}
			s.cond.Wait()
		}

		front := s.items.Front()
		item := front.Value.(sortItem)
		s.items.Remove(front)

		if item.tp == pb.BinlogType_Prewrite {
			for {
				_, ok := s.waitStarts[item.start]
				if ok == false {
					break
				}
				s.cond.Wait()
			}
		} else {
			if item.commit > maxTSItem.commit {
				maxTSItem = item
				s.maxTSItemCB(maxTSItem)
			}
		}
		s.cond.L.Unlock()
	}
}

func (s *sorter) close() {
	atomic.StoreInt32(&s.isClosed, 1)
	// let run() await and quit when items.Len() = 0
	s.cond.Broadcast()
}
