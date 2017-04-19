package heap

import (
	"container/heap"
)

type resultMsg struct {
	result chan interface{}
}

// SafeHeap is a thread safe heap
// use one goroutine to serialize the operators
type SafeHeap struct {
	h        heap.Interface
	pushChan chan interface{}
	popChan  chan *resultMsg

	closed chan struct{}
}

// NewSafeHeap returns a SafeHeap
func NewSafeHeap(h heap.Interface) *SafeHeap {
	sh := &SafeHeap{
		h:        h,
		pushChan: make(chan interface{}),
		popChan:  make(chan *resultMsg),
		closed:   make(chan struct{}),
	}

	sh.watchOps()
	return sh
}

// Push pushs an item into the SafeHeap
func (sh *SafeHeap) Push(item interface{}) {
	sh.pushChan <- item
}

// Pop pops an item from the SafeHeap
func (sh *SafeHeap) Pop() interface{} {
	result := make(chan interface{})

	sh.popChan <- &resultMsg{
		result: result,
	}
	return <-result
}

//Stop stops the SafeHeap
func (sh *SafeHeap) Stop() {
	sh.closed <- struct{}{}
}

func (sh *SafeHeap) watchOps() {
	go func() {
		for {
			select {
			case <-sh.closed:
				return
			case popMsg := <-sh.popChan:
				if sh.h.Len() == 0 {
					popMsg.result <- nil
				} else {
					popMsg.result <- heap.Pop(sh.h)
				}
			case pushMsg := <-sh.pushChan:
				heap.Push(sh.h, pushMsg)
			}
		}
	}()
}
