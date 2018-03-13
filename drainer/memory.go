package drainer

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/mem"
	"github.com/shirou/gopsutil/process"
)

func reachMemoryLimit(ps *process.Process, maxMemUsed, maxMemPercent uint64) (bool, error) {
	memUsed, memPercent, err := mem.GetMemoryState(ps)
	if err != nil {
		return false, errors.Trace(err)
	}
	log.Debugf("memory used: %d, percent: %d", memUsed, memPercent)
	if memUsed > maxMemUsed || memPercent > maxMemPercent {
		log.Warnf("drainer use too many memory, used: %d, percent: %d", memUsed, memPercent)
		return true, nil
	}

	return false, nil
}
