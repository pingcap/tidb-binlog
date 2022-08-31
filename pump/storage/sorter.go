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
	"container/list"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	pb "github.com/pingcap/tipb/go-binlog"
)

// a test helper, generate n pair item
func newItemGenerator(txnNum int32, maxLatency int64, fakeTxnPerNum int32) <-chan sortItem {
	items := make(chan sortItem)
	oracle := newMemOracle()

	const threadNum = 10

	var wg sync.WaitGroup

	for i := 0; i < threadNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				num := atomic.AddInt32(&txnNum, -1)
				if num < 0 {
					return
				}

				if fakeTxnPerNum > 0 && num%fakeTxnPerNum == 0 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						ts := oracle.getTS()
						items <- sortItem{
							start:  ts,
							commit: ts,
							tp:     pb.BinlogType_Rollback,
						}
					}()
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

				time.Sleep(time.Microsecond * time.Duration(rand.Int63()%maxLatency))
			}

		}()
	}

	go func() {
		wg.Wait()
		close(items)
	}()

	return items
}

// / sorter
type sortItem struct {
	start  int64
	commit int64
	tp     pb.BinlogType
}

// String implements fmt.Stringer
func (s *sortItem) String() string {
	return fmt.Sprintf("{start: %d, commit: %d, tp: %s}", s.start, s.commit, s.tp)
}

type sorter struct {
	maxTSItemCB func(item sortItem)
	// if resolver return true, we can skip the P binlog, don't need to wait for the C binlog
	resolver func(startTS int64) bool
	// save the startTS of txn which we need to wait for the C binlog
	waitStartTS map[int64]struct{}

	lock   sync.Mutex
	cond   *sync.Cond
	items  *list.List
	wg     sync.WaitGroup
	closed int32
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

func (s *sorter) allMatched() bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	return len(s.waitStartTS) == 0
}

func (s *sorter) pushTSItem(item sortItem) {
	if s.isClosed() {
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

	go func() {
		// Avoid if no any more pushTSItem call so block at s.cond.Wait() in run() waiting the matching c-binlog
		tick := time.NewTicker(1 * time.Second)
		defer tick.Stop()
		for range tick.C {
			s.cond.Signal()
			if s.isClosed() {
				return
			}
		}
	}()

	var maxTSItem sortItem
	for {
		s.cond.L.Lock()
		for s.items.Len() == 0 {
			if s.isClosed() {
				s.cond.L.Unlock()
				return
			}
			s.cond.Wait()
		}

		front := s.items.Front()
		item := front.Value.(sortItem)
		s.items.Remove(front)

		if item.tp == pb.BinlogType_Prewrite {
			getTime := time.Now()

			for {
				_, ok := s.waitStartTS[item.start]
				if !ok {
					break
				}

				// we may get the C binlog soon at start up time
				if time.Since(getTime) > time.Second {
					if s.resolver != nil && s.resolver(item.start) {
						delete(s.waitStartTS, item.start)
						break
					}
				}

				// quit if sorter is closed and still not get the according C binlog
				// or continue to handle the items
				if s.isClosed() {
					s.cond.L.Unlock()
					return
				}
				s.cond.Wait()
			}
		} else {
			// commit is greater than zero, it's OK when we get the first item and maxTSItem.commit = 0
			if item.commit > maxTSItem.commit {
				maxTSItem = item
				s.maxTSItemCB(maxTSItem)
			}
		}
		s.cond.L.Unlock()
	}
}

func (s *sorter) isClosed() bool {
	return atomic.LoadInt32(&s.closed) == 1
}

func (s *sorter) close() {
	atomic.StoreInt32(&s.closed, 1)
	// let run() await and quit when items.Len() = 0
	s.cond.L.Lock()
	s.cond.Broadcast()
	s.cond.L.Unlock()

	s.wg.Wait()
}
