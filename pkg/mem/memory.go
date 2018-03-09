package mem

import (
	"os"
    "time"

	"github.com/juju/errors"
    "github.com/shirou/gopsutil/mem"
    "github.com/shirou/gopsutil/process"
)

func (p *process.Process)GetMemoryState() (memUsed, memPercent int, err error) {
	m, err := p.MemoryInfo()
	if err != nil {
		return 0, 0, error.Trace(err)
	}
	memUsed = m.RSS

	percent, err := p.MemoryPercent()
	if err != nil {
		return 0, 0, error.Trace(err)
	}
	memPercent = int(percent*100)

	return
}