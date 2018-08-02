package drainer

 import (
 	"math"
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
 	binlogs map[string]MergeItem
 	chans   map[string]chan MergeItem

 	minBinlog MergeItem
 	mindID    string

 	output chan MergeItem

 	toBeAddMu sync.Mutex
 	toBeAdd   []MergeSource

 	// when close, close the output chan once chans is empty
 	close int32
 }

 // MergeSource contains a source info about binlog
 type MergeSource struct {
 	ID     string
 	Source chan MergeItem
 }

 // NewMerger create a instance of Merger
 func NewMerger(sources ...MergeSource) *Merger {
 	m := &Merger{
 		binlogs: make(map[string]MergeItem),
 		chans:   make(map[string]chan MergeItem),
 		output:  make(chan MergeItem, 10),
 	}

 	for i := 0; i < len(sources); i++ {
 		m.chans[sources[i].ID] = sources[i].Source
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
 	m.toBeAddMu.Lock()
 	m.toBeAdd = append(m.toBeAdd, source)
 	m.toBeAddMu.Unlock()
 }

 func (m *Merger) run() {
 	defer close(m.output)

 	for {
 		var lastTS int64
 		lastTS = math.MinInt64

 		m.toBeAddMu.Lock()
 		for _, source := range m.toBeAdd {
 			m.chans[source.ID] = source.Source
 		}
 		m.toBeAdd = m.toBeAdd[:0]
 		m.toBeAddMu.Unlock()

 		var minBinlog MergeItem
 		var minID string

 		for id, c := range m.chans {
 			if _, ok := m.binlogs[id]; ok {
 				continue
 			}

 			binlog, ok := <-c
 			if ok == false {
 				delete(m.chans, id)
 			} else {
 				m.binlogs[id] = binlog
 			}
 		}

 		if len(m.chans) == 0 {
 			if m.isClosed() {
 				return
 			}
 			time.Sleep(time.Second)
 			continue
 		}

 		for id, binlog := range m.binlogs {
 			if minBinlog == nil || binlog.GetCommitTs() < minBinlog.GetCommitTs() {
 				minBinlog = binlog
 				minID = id
 			}
 		}

 		delete(m.binlogs, minID)
 		if minBinlog.GetCommitTs() <= lastTS {
 			continue
 		} else {
 			m.output <- minBinlog
 			lastTS = minBinlog.GetCommitTs()
 		}
 	}
 }

 // Output get the output chan of binlog
 func (m *Merger) Output() chan MergeItem {
 	return m.output
 }