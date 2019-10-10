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
	"container/heap"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	normalStrategy = "normal"
	heapStrategy   = "heap"
)

// MergeItem is the item in Merger
type MergeItem interface {
	GetCommitTs() int64

	GetSourceID() string
}

// MergeItems is a heap of MergeItems.
type MergeItems []MergeItem

func (m MergeItems) Len() int           { return len(m) }
func (m MergeItems) Less(i, j int) bool { return m[i].GetCommitTs() < m[j].GetCommitTs() }
func (m MergeItems) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }

// Push implements heap.Interface's Push function
func (m *MergeItems) Push(x interface{}) {
	*m = append(*m, x.(MergeItem))
}

// Pop implements heap.Interface's Pop function
func (m *MergeItems) Pop() interface{} {
	old := *m
	n := len(old)
	x := old[n-1]
	*m = old[0 : n-1]
	return x
}

// MergeStrategy is a strategy interface for merge item
type MergeStrategy interface {
	Push(MergeItem)
	Pop() MergeItem
	Exist(string) bool
}

// HeapStrategy is a strategy to get min item using heap
type HeapStrategy struct {
	items     *MergeItems
	sourceIDs map[string]interface{}
}

// NewHeapStrategy returns a new HeapStrategy
func NewHeapStrategy() *HeapStrategy {
	h := &HeapStrategy{
		items:     new(MergeItems),
		sourceIDs: make(map[string]interface{}),
	}
	heap.Init(h.items)
	return h
}

// Push implements MergeStrategy's Push function
func (h *HeapStrategy) Push(item MergeItem) {
	if _, ok := h.sourceIDs[item.GetSourceID()]; ok {
		log.Error("should not push item", zap.String("sourceID", item.GetSourceID()))
	}

	heap.Push(h.items, item)
	h.sourceIDs[item.GetSourceID()] = struct{}{}
}

// Pop implements MergeStrategy's Pop function
func (h *HeapStrategy) Pop() MergeItem {
	if h.items.Len() == 0 {
		log.Error("no item exist")
		return nil
	}

	item := heap.Pop(h.items).(MergeItem)
	delete(h.sourceIDs, item.GetSourceID())
	return item
}

// Exist implements MergeStrategy's Exist function
func (h *HeapStrategy) Exist(sourceID string) bool {
	_, ok := h.sourceIDs[sourceID]
	return ok
}

// NormalStrategy is a strategy to get min item using normal way
type NormalStrategy struct {
	items map[string]MergeItem
}

// NewNormalStrategy returns a new NormalStrategy
func NewNormalStrategy() *NormalStrategy {
	return &NormalStrategy{
		items: make(map[string]MergeItem),
	}
}

// Push implements MergeStrategy's Push function
func (n *NormalStrategy) Push(item MergeItem) {
	if _, ok := n.items[item.GetSourceID()]; ok {
		log.Error("should not push item", zap.String("source id", item.GetSourceID()))
	}
	n.items[item.GetSourceID()] = item
}

// Pop implements MergeStrategy's Pop function
func (n *NormalStrategy) Pop() MergeItem {
	var minItem MergeItem
	for _, item := range n.items {
		if minItem == nil || item.GetCommitTs() < minItem.GetCommitTs() {
			minItem = item
		}
	}
	if minItem == nil {
		log.Error("no item exist")
		return nil
	}

	delete(n.items, minItem.GetSourceID())
	return minItem
}

// Exist implements MergeStrategy's Exist function
func (n *NormalStrategy) Exist(sourceID string) bool {
	_, ok := n.items[sourceID]
	return ok
}

// Merger do merge sort of binlog
type Merger struct {
	sync.RWMutex

	sources map[string]MergeSource

	strategy MergeStrategy

	output chan MergeItem

	// latestTS save the last binlog's ts send to syncer
	latestTS int64

	close int32

	pause int32

	sourceChanged int32
}

// MergeSource contains a source info about binlog
type MergeSource struct {
	ID     string
	Source chan MergeItem
}

// NewMerger creates a instance of Merger
func NewMerger(ts int64, strategy string, sources ...MergeSource) *Merger {
	var mergeStrategy MergeStrategy
	switch strategy {
	case heapStrategy:
		mergeStrategy = NewHeapStrategy()
	case normalStrategy:
		mergeStrategy = NewNormalStrategy()
	default:
		log.Error("unsupport strategy, use heap instead", zap.String("strategy", strategy))
		mergeStrategy = NewHeapStrategy()
	}

	m := &Merger{
		latestTS: ts,
		sources:  make(map[string]MergeSource),
		output:   make(chan MergeItem),
		strategy: mergeStrategy,
	}

	for i := 0; i < len(sources); i++ {
		m.sources[sources[i].ID] = sources[i]
	}

	go m.run()

	return m
}

// Close close the output chan when all the source id drained
func (m *Merger) Close() {
	log.Debug("close merger")
	atomic.StoreInt32(&m.close, 1)
}

func (m *Merger) isClosed() bool {
	return atomic.LoadInt32(&m.close) == 1
}

// AddSource add a source to Merger
func (m *Merger) AddSource(source MergeSource) {
	m.Lock()
	m.sources[source.ID] = source
	log.Info("merger add source", zap.String("source id", source.ID))
	m.setSourceChanged()
	m.Unlock()
}

// RemoveSource removes a source from Merger
func (m *Merger) RemoveSource(sourceID string) {
	m.Lock()
	delete(m.sources, sourceID)
	log.Info("merger remove source", zap.String("source id", sourceID))
	m.setSourceChanged()
	m.Unlock()
}

func (m *Merger) run() {
	defer close(m.output)

	latestTS := m.latestTS

	for {
		m.resetSourceChanged()

		if m.isClosed() {
			log.Info("Merger is closed successfully")
			return
		}

		if m.isPaused() {
			time.Sleep(time.Second)
			continue
		}

		skip := false
		sources := make(map[string]MergeSource)
		m.RLock()
		for sourceID, source := range m.sources {
			sources[sourceID] = source
		}
		m.RUnlock()

		if len(sources) == 0 {
			// don't have any source
			time.Sleep(time.Second)
			continue
		}

		for sourceID, source := range sources {
			if m.strategy.Exist(sourceID) {
				continue
			}

			if source.Source == nil {
				log.Warn("can't read binlog from pump", zap.String("source id", sourceID))
				skip = true
			} else {
				binlog, ok := <-source.Source
				if ok {
					m.Lock()
					m.strategy.Push(binlog)
					m.Unlock()
				} else {
					// the source is closing.
					log.Warn("can't read binlog from pump", zap.String("source id", sourceID))
					skip = true
				}
			}
		}

		if skip {
			// can't get binlog from all source, so can't run merge sort.
			// maybe the source is offline, and then collector will remove this pump in this case.
			// or meet some error, the pump's ctx is done, and drainer will exit.
			// so just wait a second and continue.
			time.Sleep(time.Second)
			continue
		}

		var minBinlog MergeItem

		m.RLock()
		minBinlog = m.strategy.Pop()
		m.RUnlock()

		if minBinlog == nil {
			continue
		}

		// may add new source, or remove source, need choose a new min binlog
		if m.isSourceChanged() {
			// push the min binlog back
			m.Lock()
			m.strategy.Push(minBinlog)
			m.Unlock()
			continue
		}

		minBinlogTS := minBinlog.GetCommitTs()
		if minBinlogTS < latestTS {
			disorderBinlogCount.Add(1)
			log.Error("binlog's commit ts less than the last ts",
				zap.Int64("commit ts", minBinlogTS),
				zap.Int64("last ts", latestTS))
		} else if minBinlogTS == latestTS {
			log.Warn("duplicate binlog", zap.Int64("commit ts", minBinlogTS))
		} else {
			m.output <- minBinlog
			latestTS = minBinlogTS
		}

		m.Lock()
		m.latestTS = latestTS
		m.Unlock()
	}
}

// Output get the output chan of binlog
func (m *Merger) Output() chan MergeItem {
	return m.output
}

// GetLatestTS returns the last binlog's ts send to syncer
func (m *Merger) GetLatestTS() int64 {
	m.RLock()
	defer m.RUnlock()
	return m.latestTS
}

// Stop stops merge
func (m *Merger) Stop() {
	atomic.StoreInt32(&m.pause, 1)
}

// Continue continue merge
func (m *Merger) Continue() {
	atomic.StoreInt32(&m.pause, 0)
}

func (m *Merger) isPaused() bool {
	return atomic.LoadInt32(&m.pause) == 1
}

func (m *Merger) setSourceChanged() {
	atomic.StoreInt32(&m.sourceChanged, 1)
}

func (m *Merger) resetSourceChanged() {
	atomic.StoreInt32(&m.sourceChanged, 0)
}

func (m *Merger) isSourceChanged() bool {
	return atomic.LoadInt32(&m.sourceChanged) == 1
}
