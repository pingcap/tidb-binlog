package mem

import (
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/shirou/gopsutil/process"
)

type MemoryControl struct {
	mu sync.RWMutex
	MaxSize  uint64
	UsedSize uint64
}

func NewMemoryControl(maxSize uint64) *MemoryControl {
	return &MemoryControl{
		MaxSize:  maxSize,
		UsedSize: 0,
	}
}

func (m *MemoryControl)AllocMemory(allocSize uint64) (reach bool, usedPercent float32)  {
	//if allocSize > (m.MaxSize - m.UsedSize) {
	//	return errors.Errorf("not enough free memory for allocate! allocate %d bytes memory", allocSize)
	//}
	m.mu.Lock()
	defer m.mu.Unlock()

	m.UsedSize += allocSize
	reach = m.UsedSize > m.MaxSize
	usedPercent = float32(m.UsedSize)/float32(m.MaxSize)
	log.Infof("memory used: %d, max: %d, alloc: %d", m.UsedSize, m.MaxSize, allocSize)
	return
}

func (m *MemoryControl)FreeMemory(freeSize uint64) {
	//if m.UsedSize > freeSize {
	//	return errors.Errorf("free memory failed! free memory bytes: %d", freeSize)
	//}
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Infof("free memory used: %d, free: %d", m.UsedSize, freeSize)
	m.UsedSize -= freeSize
	//return nil
}

func (m *MemoryControl)GetStatus() (uint64, uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.MaxSize, m.UsedSize
}

// GetMemoryState get the process's memory information
func GetMemoryState(p *process.Process) (memUsed, memPercent uint64, err error) {
	m, err := p.MemoryInfo()
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	memUsed = m.RSS

	percent, err := p.MemoryPercent()
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	memPercent = uint64(percent)

	return
}
