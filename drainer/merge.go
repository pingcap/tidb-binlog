package drainer

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/log"
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

type MergeStrategy interface {
	Push(*MergeItem)
	Pop() *MergeItem
	GetLastItemSourceID() string
}

type HeapStrategy struct {
	items *MergeItems
	lastItemSourceID string
}

// NewHeapStrategy returns a new HeapStrategy
func NewHeapStrategy() *HeapStrategy {
	h := &HeapStrategy {}
	heap.Init(h.items)
	return h
}

func (h *HeapStrategy) Push(item MergeItem) {
	heap.Push(h.items, item)
}

func (h *HeapStrategy) Pop() MergeItem {
	item = heap.Pop(m.binlogsHeap).(MergeItem)
	lastItemSourceID ＝ item.GetSourceID()
}

type NormalStrategy struct {
	items map[string]MergeItem
	lastItemSourceID string
}

func NewNormalStrategy() *NormalStrategy {
	return *NormalStrategy {
		items:  make(map[string]MergeItem)
	}
}

func (n *NormalStrategy) Push(item MergeItem) {
	if _, ok := m.items[item.GetSourceID]; ok {
		log.Errorf("")
	}
	m.items[item.GetSourceID()] = item
}

func (n *NormalStrategy) Pop() MergeItem {
	var minItem MergeItem
	for _, item := range m.items {
		if minItem == nil || item.GetCommitTs() < minItem.GetCommitTs() {
			minItem = item
		}
	}
	n.lastItemSourceID = minItem.GetSourceID()
	delete(n.items, minItem.GetSourceID())
	return minItem
}

func (n *NormalStrategy) GetLastItemSourceID() string {
	return n.lastItemSourceID
}

// Merger do merge sort of binlog
type Merger struct {
	sync.RWMutex

	sources map[string]MergeSource

	binlogs map[string]MergeItem

	binlogsHeap *MergeItems

	output chan MergeItem

	// latestTS save the last binlog's ts send to syncer
	latestTS int64

	close int32

	pause int32

	sourceChanged int32

	// strategy can be "heap" or "normal"
	strategy string
}

// MergeSource contains a source info about binlog
type MergeSource struct {
	ID     string
	Source chan MergeItem
}

// NewMerger creates a instance of Merger
func NewMerger(ts int64, strategy string, sources ...MergeSource) *Merger {
	m := &Merger{
		latestTS:    ts,
		sources:     make(map[string]MergeSource),
		output:      make(chan MergeItem, 10),
		binlogs:     make(map[string]MergeItem),
		binlogsHeap: new(MergeItems),
		strategy:    strategy,
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
	log.Infof("merger add source %s", source.ID)
	m.setSourceChanged()
	m.Unlock()
}

// RemoveSource removes a source from Merger
func (m *Merger) RemoveSource(sourceID string) {
	m.Lock()
	delete(m.sources, sourceID)
	log.Infof("merger remove source %s", sourceID)
	m.setSourceChanged()
	m.Unlock()
}

func (m *Merger) run() {
	defer close(m.output)

	heap.Init(m.binlogsHeap)
	latestTS := m.latestTS

	for {
		m.resetSourceChanged()

		if m.isClosed() {
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
			m.RLock()
			_, ok := m.binlogs[sourceID]
			m.RUnlock()

			if ok {
				continue
			}

			if source.Source == nil {
				log.Warnf("can't read binlog from pump %s", sourceID)
				skip = true
			} else {
				binlog, ok := <-source.Source
				if ok {
					m.Lock()
					if m.strategy == heapStrategy {
						heap.Push(m.binlogsHeap, binlog)
					}
					m.binlogs[sourceID] = binlog
					m.Unlock()
				} else {
					// the source is closing.
					log.Warnf("can't read binlog from pump %s", sourceID)
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
		var minID string

		m.RLock()
		if m.strategy == normalStrategy {
			for sourceID, binlog := range m.binlogs {
				if minBinlog == nil || binlog.GetCommitTs() < minBinlog.GetCommitTs() {
					minBinlog = binlog
					minID = sourceID
				}
			}
		} else {
			if m.binlogsHeap.Len() > 0 {
				minBinlog = heap.Pop(m.binlogsHeap).(MergeItem)
				minID = minBinlog.GetSourceID()
			}
		}
		m.RUnlock()

		// may add new source, or remove source, need choose a new min binlog
		if m.isSourceChanged() {
			continue
		}

		if minBinlog == nil {
			continue
		}

		if minBinlog.GetCommitTs() <= latestTS {
			// TODO: add metric here
			log.Errorf("binlog's commit ts is %d, and is greater than the last ts %d", minBinlog.GetCommitTs(), latestTS)
		} else {
			m.output <- minBinlog
			latestTS = minBinlog.GetCommitTs()
		}

		m.Lock()
		m.latestTS = latestTS
		delete(m.binlogs, minID)
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
	atomic.StoreInt32(&m.pause, 1)
}

func (m *Merger) resetSourceChanged() {
	atomic.StoreInt32(&m.pause, 0)
}

func (m *Merger) isSourceChanged() bool {
	return atomic.LoadInt32(&m.sourceChanged) == 1
}
