package mem

import (
	"github.com/juju/errors"
	"github.com/shirou/gopsutil/process"
)

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
