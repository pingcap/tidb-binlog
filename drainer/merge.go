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
	DefaultCacheSize = 1024
)

// MergeItem is the item in Merger
type MergeItem interface {
	GetCommitTs() int64
}

// Merger do merge sort of binlog
type Merger struct {
	sync.RWMutex

	//Binlogs map[string]MergeItem
	//chans   map[string]chan MergeItem

	sources map[string]MergeSource

	output chan MergeItem

	newSource      []MergeSource
	removeSource   []string
	pauseSource    []string
	continueSource []string

	// when close, close the output chan once chans is empty
	close int32

	// TODO: save the max and min binlog
	window *DepositWindow
}

// MergeSource contains a source info about binlog
type MergeSource struct {
	ID      string
	Source  chan MergeItem
	Pause   bool
	Binlogs []MergeItem
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

	// remove source
	for _, sourceID := range m.removeSource {
		delete(m.sources, sourceID)
		log.Infof("merger remove source %s", sourceID)
	}

	// pause source
	for _, sourceID := range m.pauseSource {
		if source, ok := m.sources[sourceID]; ok {
			source.Pause = true
			log.Infof("merger pause source %s", sourceID)
		}
	}

	// continue source
	for _, sourceID := range m.continueSource {
		if source, ok := m.sources[sourceID]; ok {
			source.Pause = false
			log.Infof("merger continue source %s", sourceID)
		}
	}
}

func (m *Merger) run() {
	defer close(m.output)

	for {
		m.updateSource()

		var lastTS int64
		lastTS = math.MinInt64

		binlogNum := 0

		for _, source := range m.sources {
			if source.Pause {
				continue
			}

			for len(source.Binlogs) < DefaultCacheSize {
				binlog, ok := <-source.Source
				if ok {
					binlogNum++
					source.Binlogs = append(source.Binlogs, binlog)
				} else {
					break
				}
			}
		}

		if binlogNum == 0 {
			if m.isClosed() {
				return
			}

			time.Sleep(time.Second)
			continue
		}

		finish := false
		offset := make(map[string]int)
		for id := range m.sources {
			offset[id] = 0
		}

		for {
			if finish {
				break
			}

			var minBinlog MergeItem
			var minID string

			for id, source := range m.sources {
				if offset[id]+1 > len(source.Binlogs) {
					finish = true
					break
				}

				if minBinlog == nil || source.Binlogs[offset[id]].GetCommitTs() < minBinlog.GetCommitTs() {
					minBinlog = source.Binlogs[offset[id]]
					minID = id
				}
			}

			if minBinlog == nil {
				break
			}

			offset[minID]++

			if minBinlog.GetCommitTs() <= lastTS {
				break
			}
			m.output <- minBinlog
			lastTS = minBinlog.GetCommitTs()
		}

		// delete Binlogs
		for id, source := range m.sources {
			source.Binlogs = source.Binlogs[offset[id]:]
		}
	}
}

// Output get the output chan of binlog
func (m *Merger) Output() chan MergeItem {
	return m.output
}
