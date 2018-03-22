package memory

import (
	"sync"
)

type MemoryControl struct {
	mu            sync.RWMutex
	MemoryUsedMap map[string]uint64
	NumMap        map[string]uint64
	MaxMemory     uint64
	MemoryUsed    uint64
}

func NewMemoryControl(maxMemory uint64) *MemoryControl {
	return &MemoryControl{
		MemoryUsedMap: make(map[string]uint64),
		NumMap:        make(map[string]uint64),
		MaxMemory:     maxMemory,
	}
}

func (m *MemoryControl)Allocate(size uint64, label string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if label != "" {
		m.MemoryUsedMap[label] += size
		m.NumMap[label] += 1
	}

	m.MemoryUsed += size

	if m.MemoryUsed > m.MaxMemory {
		// do something
	}
}

func (m *MemoryControl)Free(size uint64, label string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if label != "" {
		m.MemoryUsedMap[label] -= size
		m.NumMap[label] -= 1
	}
	
	m.MemoryUsed -= size
}