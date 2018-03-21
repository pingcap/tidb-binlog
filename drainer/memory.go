package drainer

import (
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/mem"
	"github.com/pingcap/tidb-binlog/pkg/flow"
	"github.com/shirou/gopsutil/process"
)                                                                                                                                                                                                                                                                                                      

type FlowControl struct {
	Mu sync.RWMutex

	MaxMemSize uint64


	Flow  *flow.SpeedControl

	//Mem   *mem.MemoryControl,
	// memory control for every pump and stage
	//MemMap map[int](map[string]*mem.MemoryControl),

	MemMap map[int]*mem.MemoryControl

	// memory used map for every pump
	PumpMemUsed map[int]uint64

	// memory used map for every stage 
	//StageMemUsed map[string]uint64
}

func NewControl(maxMemSize, rate, token, maxToken, interval uint64) *FlowControl {
	//m := mem.NewMemoryControl(maxSize)
	f := flow.NewSpeedControl(rate, token, maxToken, interval)
	return &FlowControl{
		MaxMemSize:  maxMemSize,
		Flow:        f,
	}
}

// return true if memory used is gt max memory size
func (f *FlowControl) AllocMem(pumpId int, memSize uint64) bool {
	f.Mu.RLock()
	_, ok := f.MemMap[pumpId]
	f.Mu.RUnlock()
	if !ok {
		f.Mu.Lock()
		pumpNum := uint64(len(f.MemMap) + 1)
		for pumpId, _ := range f.MemMap {
			f.MemMap[pumpId].MaxSize = f.MaxMemSize*(pumpNum-1)/pumpNum
		}
		f.MemMap[pumpId] = mem.NewMemoryControl(f.MaxMemSize/pumpNum)
		f.PumpMemUsed[pumpId] = 0
		f.Mu.Unlock()
	}

	f.PumpMemUsed[pumpId] += memSize
	return f.MemMap[pumpId].AllocMemory(memSize)
}

func (f *FlowControl) BalanceMem() {
	f.Mu.Lock()
	defer f.Mu.Unlock()

	var totalMemUsed uint64 = 0
	for _, memUsed := range f.PumpMemUsed {
		totalMemUsed += memUsed
	}

	for pumpId, memUsed := range f.PumpMemUsed {
		f.MemMap[pumpId].MaxSize = f.MaxMemSize*memUsed/totalMemUsed
	}
}




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
