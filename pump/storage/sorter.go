package storage

import (
	"container/list"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
)

// a test helper, generate n pair item
func newItemGenerator(txnNum int32) <-chan sortItem {
	items := make(chan sortItem)
	oracle := newMemOracle()

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
	// if resolver return true, we can skip the P binlog, don't need to wait for the C binlog
	resolver func(startTS int64) bool
	// save the startTS of txn which we need to wait for the C binlog
	waitStartTS map[int64]struct{}

	lock     sync.Mutex
	cond     *sync.Cond
	items    *list.List
	wg       sync.WaitGroup
	isClosed int32
}

func newSorter(fn func(item sortItem)) *sorter {
	sorter := &sorter{
		maxTSItemCB: fn,
		items:       list.New(),
		waitStartTS: make(map[int64]struct{}),
	}

	sorter.cond = sync.NewCond(&sorter.lock)
	sorter.wg.Add(1)
	go sorter.run()

	return sorter
}

func (s *sorter) setResolver(resolver func(startTS int64) bool) {
	s.resolver = resolver
}

func (s *sorter) pushTSItem(item sortItem) {
	if atomic.LoadInt32(&s.isClosed) == 1 {
		// i think we can just panic
		log.Error("sorter is closed but still push item, this should never happen")
	}

	s.lock.Lock()

	if item.tp == pb.BinlogType_Prewrite {
		s.waitStartTS[item.start] = struct{}{}
	} else {
		delete(s.waitStartTS, item.start)
		s.cond.Signal()
	}

	s.items.PushBack(item)
	if s.items.Len() == 1 {
		s.cond.Signal()
	}
	s.lock.Unlock()
}

func (s *sorter) run() {
	defer s.wg.Done()

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
				_, ok := s.waitStartTS[item.start]
				if ok == false {
					break
				}
				if s.resolver != nil && s.resolver(item.start) {
					break
				}

				// quit if sorter is closed and still not get the according C binlog
				// or continue to handle the items
				if atomic.LoadInt32(&s.isClosed) == 1 {
					return
				}
				s.cond.Wait()
			}
		} else {
			// commit is greater than zero, it's ok when we get the first item and maxTSItem.commit = 0
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
	s.cond.L.Lock()
	s.cond.Broadcast()
	s.cond.L.Unlock()

	s.wg.Wait()
}
