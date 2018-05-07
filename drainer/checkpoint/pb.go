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

// PbCheckPoint is local CheckPoint struct.
type PbCheckPoint struct {
	sync.RWMutex
	initialCommitTS int64

	name     string
	saveTime time.Time

	CommitTS int64 `toml:"commitTS" json:"commitTS"`
	// drainer only stores the binlog file suffix
	Positions map[string]pb.Pos `toml:"positions" json:"positions"`
}

// NewPb creates a new Pb.
func newPb(cfg *Config) (CheckPoint, error) {
	pb := &PbCheckPoint{initialCommitTS: cfg.InitialCommitTS, name: cfg.CheckPointFile, Positions: make(map[string]pb.Pos)}
	err := pb.Load()
	if err != nil {
		return pb, errors.Trace(err)
	}

	return pb, nil
}

// Load implements CheckPointor.Load interface.
func (sp *PbCheckPoint) Load() error {
	sp.Lock()
	defer func() {
		if sp.CommitTS == 0 {
			sp.CommitTS = sp.initialCommitTS
		}
		sp.Unlock()
	}()

	file, err := os.Open(sp.name)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return errors.Trace(err)
	}
	if os.IsNotExist(errors.Cause(err)) {
		sp.CommitTS = sp.initialCommitTS
		return nil
	}
	defer file.Close()

	_, err = toml.DecodeReader(file, sp)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Save implements CheckPoint.Save interface
func (sp *PbCheckPoint) Save(ts int64, poss map[string]pb.Pos) error {
	sp.Lock()
	defer sp.Unlock()

	for nodeID, pos := range poss {
		newPos := pb.Pos{}
		if pos.Offset > 5000 {
			newPos.Suffix = pos.Suffix
			newPos.Offset = pos.Offset - 5000
		}
		sp.Positions[nodeID] = newPos
	}

	sp.CommitTS = ts

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(sp)
	if err != nil {
		log.Errorf("syncer save CheckPointor info to file %s err %v", sp.name, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	err = ioutil2.WriteFileAtomic(sp.name, buf.Bytes(), 0644)
	if err != nil {
		log.Errorf("syncer save CheckPointor info to file %s err %v", sp.name, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	sp.saveTime = time.Now()
	return nil
}

// Check implements CheckPoint.Check interface
func (sp *PbCheckPoint) Check() bool {
	sp.RLock()
	defer sp.RUnlock()

	return time.Since(sp.saveTime) >= maxSaveTime
}

// Pos implements CheckPoint.Pos interface
func (sp *PbCheckPoint) Pos() (int64, map[string]pb.Pos) {
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

// String implements CheckPoint.String interface
func (sp *PbCheckPoint) String() string {
	ts, poss := sp.Pos()
	return fmt.Sprintf("binlog commitTS = %d and positions = %+v", ts, poss)
}
