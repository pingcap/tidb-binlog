package drainer

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/log"
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

	// lastTS save the last binlog's ts send to syncer
	lastTS int64

	close int32

	pause int32
}

// MergeSource contains a source info about binlog
type MergeSource struct {
	ID     string
	Source chan MergeItem
}

// NewMerger creates a instance of Merger
func NewMerger(ts int64, sources ...MergeSource) *Merger {
	m := &Merger{
		lastTS:  ts,
		sources: make(map[string]MergeSource),
		output:  make(chan MergeItem, 10),
		binlogs: make(map[string]MergeItem),
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
	m.Unlock()
}

// RemoveSource removes a source from Merger
func (m *Merger) RemoveSource(sourceID string) {
	m.Lock()
	delete(m.sources, sourceID)
	log.Infof("merger remove source %s", sourceID)
	m.Unlock()
}

func (m *Merger) run() {
	defer close(m.output)

	lastTS := m.lastTS
	for {
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
		for sourceID, binlog := range m.binlogs {
			if minBinlog == nil || binlog.GetCommitTs() < minBinlog.GetCommitTs() {
				minBinlog = binlog
				minID = sourceID
			}
		}

		if len(m.binlogs) != len(m.sources) {
			// had new source, should choose a new min binlog again
			m.RUnlock()
			continue
		}
		m.RUnlock()

		isValideBinlog := true

		if minBinlog == nil {
			isValideBinlog = false
		}

		if minBinlog != nil && minBinlog.GetCommitTs() <= lastTS {
			log.Errorf("binlog's commit ts is %d, and is greater than the last ts %d", minBinlog.GetCommitTs(), lastTS)
			isValideBinlog = false
		}

		if isValideBinlog {
			m.output <- minBinlog
			lastTS = minBinlog.GetCommitTs()
		}
		m.Lock()
		m.lastTS = lastTS
		delete(m.binlogs, minID)
		m.Unlock()
	}
}

// Output get the output chan of binlog
func (m *Merger) Output() chan MergeItem {
	return m.output
}

// GetLastTS returns the last binlog's ts send to syncer
func (m *Merger) GetLastTS() int64 {
	m.RLock()
	defer m.RUnlock()
	return m.lastTS
}

// IsEmpty returns true if this Merger don't have any binlog to be merged
func (m *Merger) IsEmpty() bool {
	m.RLock()
	defer m.RUnlock()

	if len(m.binlogs) != 0 {
		return false
	}

	return true
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
