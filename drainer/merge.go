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

	newSource      []MergeSource
	removeSource   []string
	pauseSource    []string
	continueSource []string

	// when close, close the output chan once chans is empty
	close int32

	// TODO: save the max and min binlog's ts
	window *DepositWindow
}

// MergeSource contains a source info about binlog
type MergeSource struct {
	ID     string
	Source chan MergeItem
	Pause  bool
}

// NewMerger create a instance of Merger
func NewMerger(sources ...MergeSource) *Merger {
	m := &Merger{
		sources: make(map[string]MergeSource),
		output:  make(chan MergeItem, 10),
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
	if _, ok := m.sources[source.ID]; !ok {
		m.newSource = append(m.newSource, source)
	}
	m.Unlock()
}

// RemoveSource remove a source from Merger
func (m *Merger) RemoveSource(sourceID string) {
	m.Lock()
	if _, ok := m.sources[sourceID]; ok {
		m.removeSource = append(m.removeSource, sourceID)
	}
	m.Unlock()
}

// PauseSource sets the source to pause
func (m *Merger) PauseSource(sourceID string) {
	m.Lock()
	if source, ok := m.sources[sourceID]; ok {
		if !source.Pause {
			m.pauseSource = append(m.pauseSource, sourceID)
		}
	}
	m.Unlock()
}

// ContinueSource restart the source
func (m *Merger) ContinueSource(sourceID string) {
	m.Lock()
	if source, ok := m.sources[sourceID]; ok {
		if source.Pause {
			m.continueSource = append(m.continueSource, sourceID)
		}
	}
	m.Unlock()
}

func (m *Merger) updateSource() {
	m.Lock()
	defer m.Unlock()

	// add new source
	for _, source := range m.newSource {
		m.sources[source.ID] = source
		log.Infof("merger add source %s", source.ID)
	}
	m.newSource = m.newSource[:0]

	// remove source
	for _, sourceID := range m.removeSource {
		delete(m.sources, sourceID)
		log.Infof("merger remove source %s", sourceID)
	}
	m.removeSource = m.removeSource[:0]

	// pause source
	for _, sourceID := range m.pauseSource {
		if source, ok := m.sources[sourceID]; ok {
			source.Pause = true
			log.Infof("merger pause source %s", sourceID)
		}
	}
	m.pauseSource = m.pauseSource[:0]

	// continue source
	for _, sourceID := range m.continueSource {
		if source, ok := m.sources[sourceID]; ok {
			source.Pause = false
			log.Infof("merger continue source %s", sourceID)
		}
	}
	m.continueSource = m.continueSource[:0]
}

func (m *Merger) run() {
	defer close(m.output)

	var lastTS int64 = math.MinInt64
	for {
		if m.isClosed() {
			return
		}

		m.updateSource()

		skip := false
		for sourceID, source := range m.sources {
			if source.Pause {
				skip = true
				continue
			}

			if _, ok := m.binlogs[sourceID]; ok {
				continue
			}

			binlog, ok := <-source.Source
			if ok {
				m.binlogs[sourceID] = binlog
			} else {
				// the channel is close, maybe the source is closing.
				skip = true
			}
		}

		if skip {
			// has paused source, or can't get binlog, so can't run merge sort.
			time.Sleep(time.Second)
			continue
		}

		var minBinlog MergeItem
		var minID string

		for sourceID, binlog := range m.binlogs {
			if minBinlog == nil || binlog.GetCommitTs() < minBinlog.GetCommitTs() {
				minBinlog = binlog
				minID = sourceID
			}
		}

		if minBinlog == nil {
			continue
		}

		if minBinlog.GetCommitTs() <= lastTS {
			log.Errorf("binlog's commit ts is %d, and is greater than the last ts %d", minBinlog.GetCommitTs(), lastTS)
			continue
		}

		m.output <- minBinlog
		delete(m.binlogs, minID)
		lastTS = minBinlog.GetCommitTs()
	}
}

// Output get the output chan of binlog
func (m *Merger) Output() chan MergeItem {
	return m.output
}
