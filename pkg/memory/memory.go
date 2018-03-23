package memory

import (
	"sync"
	"time"
)

type MemoryControl struct {
	mu sync.RWMutex
	// used for token
	mut sync.RWMutex

	MaxMemory  uint64
	MemoryUsed uint64

	MemoryUsedMap map[string]uint64
	NumMap        map[string]uint64
	MaxMemoryMap  map[string]uint64

	// if memory reach the max memory, use the memory token temporary
	GenTokenRate   uint64
	MemoryToken    uint64
	MaxMemoryToken uint64

	MemoryTokenMap map[string]uint64
}

func NewMemoryControl(maxMemory, maxMemoryToken, tokenRate uint64) *MemoryControl {
	m := &MemoryControl{
		MemoryUsedMap: make(map[string]uint64),
		NumMap:        make(map[string]uint64),
		MaxMemoryMap:  make(map[string]uint64),
		MaxMemory:     maxMemory,

		GenTokenRate:   tokenRate,
		MaxMemoryToken: maxMemoryToken,
	}

	go m.background()

	return m
}

func (m *MemoryControl) Allocate(size uint64, label string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if label != "" {
		m.addNewLabel(label)
		m.MemoryUsedMap[label] += size
		m.NumMap[label] += 1

		if m.MemoryUsedMap[label] > m.MaxMemoryMap[label] {
			m.applyTokenSync(label, size)
		}
	} else {
		m.MemoryUsed += size
		if m.MemoryUsed > m.MaxMemory {
			m.applyTokenSync(label, size)
		}
	}
}

func (m *MemoryControl) Free(size uint64, label string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if label != "" {
		m.MemoryUsedMap[label] -= size
		m.NumMap[label] -= 1
	} else {
		m.MemoryUsed -= size
	}
}

func (m *MemoryControl) balanceMem(average bool) {
	if len(m.MemoryUsedMap) == 0 {
		return
	}

	// use basicMemory avoid some label's max memory is too small
	labelNum := uint64(len(m.MemoryUsedMap))
	basicMemory := m.MaxMemory / labelNum / 2

	for label, memory := range m.MemoryUsedMap {
		if average {
			m.MemoryUsedMap[label] = m.MaxMemory / labelNum
		} else {
			m.MemoryUsedMap[label] = basicMemory + m.MaxMemory/2*(memory/m.MemoryUsed)
		}
	}
}

func (m *MemoryControl) OfflineLabel(label string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.MemoryUsedMap, label)
	delete(m.MaxMemoryMap, label)
	delete(m.NumMap, label)
	delete(m.MemoryTokenMap, label)
	m.balanceMem(true)
}

func (m *MemoryControl) addNewLabel(label string) {
	_, ok := m.MemoryUsedMap[label]
	if ok {
		return
	}

	m.MemoryUsedMap[label] = 0
	m.NumMap[label] = 0
	m.MemoryTokenMap[label] = 0
	m.MaxMemoryMap[label] = 0
	m.balanceMem(true)
}

func (m *MemoryControl) background() {
	// time1 is used for award memory token
	timer1 := time.NewTicker(time.Second)
	// time2 is used for balance memory between label
	timer2 := time.NewTicker(time.Hour)
	defer timer1.Stop()
	defer timer2.Stop()

	for {
		select {
		case <-timer1.C:
			m.mut.Lock()
			m.awardToken()
			m.mut.Unlock()
		case <-timer2.C:
			m.mu.Lock()
			m.balanceMem(true)
			m.mu.Unlock()
		default:
		}
	}
}

func (m *MemoryControl) awardToken() {
	labelNum := uint64(len(m.MemoryTokenMap))
	for label, token := range m.MemoryTokenMap {
		if token+m.GenTokenRate/labelNum > m.MaxMemoryToken/labelNum {
			m.MemoryTokenMap[label] = m.MaxMemoryToken / labelNum
		} else {
			m.MemoryTokenMap[label] = token + m.GenTokenRate/labelNum
		}
	}

	if m.MemoryToken+m.GenTokenRate > m.MaxMemoryToken {
		m.MemoryToken = m.MaxMemoryToken
	} else {
		m.MemoryToken += m.GenTokenRate
	}
}

func (m *MemoryControl) applyToken(label string, size uint64) bool {
	m.mut.Lock()
	defer m.mut.Unlock()

	if label != "" {
		if m.MemoryTokenMap[label] > size {
			m.MemoryTokenMap[label] -= size
			return true
		}
		return false
	}

	if m.MemoryToken > size {
		m.MemoryToken -= size
		return true
	}
	return false
}

func (m *MemoryControl) applyTokenSync(label string, size uint64) {
	for {
		labelSize := uint64(len(m.MemoryTokenMap))
		if labelSize == 0 {
			labelSize = 1
		}
		if size > m.MaxMemoryToken/labelSize {
			time.Sleep(time.Duration(2*size/m.MaxMemoryToken) * time.Second)
			m.applyToken(label, m.MaxMemoryToken/labelSize)
			break
		}

		if m.applyToken(label, size) {
			break
		}

		time.Sleep(time.Second)
	}

}
