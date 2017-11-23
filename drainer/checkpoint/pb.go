package checkpoint

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/siddontang/go/ioutil2"
)

var (
	maxSaveTime = 30 * time.Second
)

// PbSavePoint is local SaveCheckPointor struct.
type PbSavePoint struct {
	sync.RWMutex

	name     string
	saveTime time.Time

	CommitTS int64 `toml:"commitTS" json:"commitTS"`
	// drainer only stores the binlog file suffix
	Positions map[string]pb.Pos `toml:"positions" json:"positions"`
}

// NewPbSavePoint creates a new PbSavePoint.
func newPbSavePoint(cfg *DBConfig) (SaveCheckPoint, error) {
	return &PbSavePoint{name: cfg.Name, Positions: make(map[string]pb.Pos)}, nil
}

// Load implements SaveCheckPointor.Load interface.
func (sp *PbSavePoint) Load() error {
	file, err := os.Open(sp.name)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return errors.Trace(err)
	}
	if os.IsNotExist(errors.Cause(err)) {
		return nil
	}
	defer file.Close()

	_, err = toml.DecodeReader(file, sp)
	return errors.Trace(err)
}

// Save checkpoint into file
func (sp *PbSavePoint) Save(ts int64, poss map[string]pb.Pos) error {
	sp.Lock()
	defer sp.Unlock()

	for nodeID, pos := range poss {
		newPos := pb.Pos{}
		if pos.Offset > 5000 {
			newPos.Offset = pos.Offset - 5000
		}
		sp.Positions[nodeID] = newPos
	}

	sp.CommitTS = ts

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(sp)
	if err != nil {
		log.Errorf("syncer save SaveCheckPointor info to file %s err %v", sp.name, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	err = ioutil2.WriteFileAtomic(sp.name, buf.Bytes(), 0644)
	if err != nil {
		log.Errorf("syncer save SaveCheckPointor info to file %s err %v", sp.name, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	sp.saveTime = time.Now()
	return nil
}

// Check we should save checkpoint
func (sp *PbSavePoint) Check() bool {
	sp.RLock()
	defer sp.RUnlock()

	if time.Since(sp.saveTime) >= maxSaveTime {
		return true
	}

	return false
}

// Pos return checkpoint information
func (sp *PbSavePoint) Pos() (int64, map[string]pb.Pos) {
	sp.RLock()
	defer sp.RUnlock()

	poss := make(map[string]pb.Pos)
	for nodeID, pos := range sp.Positions {
		poss[nodeID] = pb.Pos{
			Suffix: pos.Suffix,
			Offset: pos.Offset,
		}
	}
	return sp.CommitTS, poss
}

// return string Pos
func (sp *PbSavePoint) String() string {
	ts, poss := sp.Pos()
	return fmt.Sprintf("binlog %s commitTS = %d positions = %+v", sp.name, ts, poss)
}
