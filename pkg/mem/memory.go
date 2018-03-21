package mem

import (
	"github.com/juju/errors"
	"github.com/shirou/gopsutil/process"
)

type MemoryControl struct {
	MaxSize  uint64
	UsedSize uint64
}

func NewMemoryControl(maxSize uint64) *MemoryControl {
	return &MemoryControl{
		MaxSize:  maxSize,
		UsedSize: 0,
	}
}

func (m *MemoryControl)AllocMemory(allocSize uint64) bool {
	//if allocSize > (m.MaxSize - m.UsedSize) {
	//	return errors.Errorf("not enough free memory for allocate! allocate %d bytes memory", allocSize)
	//}

	m.UsedSize += allocSize
	return m.UsedSize > m.MaxSize
}

func (m *MemoryControl)FreeMemory(freeSize uint64) error {
	if m.UsedSize > freeSize {
		return errors.Errorf("free memory failed! free memory bytes: %d", freeSize)
	}

	m.UsedSize -= freeSize
	return nil
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
