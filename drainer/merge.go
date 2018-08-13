package drainer

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/log"
)

const (
	// DefaultCacheSize is the default cache size for every source.
	DefaultCacheSize = 10
)

// MergeItem is the item in Merger
type MergeItem interface {
	GetCommitTs() int64
}

// Merger do merge sort of binlog
type Merger struct {
	sync.RWMutex

	sources map[string]MergeSource

	binlogs map[string]MergeItem

	output chan MergeItem

	close int32

	// TODO: save the max and min binlog's ts
	window *DepositWindow
}

// MergeSource contains a source info about binlog
type MergeSource struct {
	ID     string
	Source chan MergeItem
}

// NewMerger create a instance of Merger
func NewMerger(sources ...MergeSource) *Merger {
	m := &Merger{
		sources: make(map[string]MergeSource),
		output:  make(chan MergeItem, 10),
		binlogs: make(map[string]MergeItem),
		window:  &DepositWindow{},
	}

	for i := 0; i < len(sources); i++ {
		m.sources[sources[i].ID] = sources[i]
	}

	go m.run()

	return m
}

// Close close the outpu chan when all the source id drained
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
	m.Unlock()
}

// RemoveSource remove a source from Merger
func (m *Merger) RemoveSource(sourceID string) {
	m.Lock()
	delete(m.sources, sourceID)
	log.Infof("merger remove source %s", sourceID)
	m.Unlock()
}

func (m *Merger) run() {
	defer close(m.output)

	var lastTS int64 = math.MinInt64
	for {
		if m.isClosed() {
			return
		}

		skip := false
		sources := make(map[string]MergeSource)
		m.RLock()
		for sourceID, source := range m.sources {
			sources[sourceID] = source
		}
		m.RUnlock()

		for sourceID, source := range sources {
			m.RLock()
			_, ok := m.binlogs[sourceID]
			m.RUnlock()

			if ok {
				continue
			}

			if source.Source == nil {
				continue
			}

			binlog, ok := <-source.Source
			if ok {
				m.Lock()
				m.binlogs[sourceID] = binlog
				m.Unlock()
			} else {
				// the source is closing.
				log.Warnf("can't read binlog from pump %s", sourceID)
				skip = true
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
		for sourceID, binlog := range m.binlogs {
			if minBinlog == nil || binlog.GetCommitTs() < minBinlog.GetCommitTs() {
				minBinlog = binlog
				minID = sourceID
			}
		}
		m.RUnlock()

		if minBinlog == nil {
			continue
		}

		if minBinlog.GetCommitTs() <= lastTS {
			log.Errorf("binlog's commit ts is %d, and is greater than the last ts %d", minBinlog.GetCommitTs(), lastTS)
			continue
		}

		m.output <- minBinlog
		m.Lock()
		delete(m.binlogs, minID)
		m.Unlock()
		lastTS = minBinlog.GetCommitTs()
	}
}

// Output get the output chan of binlog
func (m *Merger) Output() chan MergeItem {
	return m.output
}
