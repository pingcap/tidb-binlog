package drainer

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

// Meta is the binlog sync pos meta.
// When syncer restarts, we should reload meta info to guarantee continuous transmission.
type Meta interface {
	// Load loads meta information.
	Load() error

	// Save saves meta information.
	Save(int64, map[string]pb.Pos) error

	// Check checks whether we should save meta.
	Check() bool

	// Pos gets position information.
	Pos() (int64, map[string]pb.Pos)
}

// LocalMeta is local meta struct.
type localMeta struct {
	sync.RWMutex

	name     string
	saveTime time.Time

	CommitTS int64             `toml:"commitTS" json:"commitTS"`
	Suffixs  map[string]uint64 `toml:"suffixs" json:"suffixs"`
}

// NewLocalMeta creates a new LocalMeta.
func NewLocalMeta(name string) Meta {
	return &localMeta{name: name}
}

// Load implements Meta.Load interface.
func (lm *localMeta) Load() error {
	file, err := os.Open(lm.name)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return errors.Trace(err)
	}
	if os.IsNotExist(errors.Cause(err)) {
		return nil
	}
	defer file.Close()

	_, err = toml.DecodeReader(file, lm)
	return errors.Trace(err)
}

// Save implements Meta.Save interface.
func (lm *localMeta) Save(ts int64, poss map[string]pb.Pos) error {
	lm.Lock()
	defer lm.Unlock()

	suffixs := make(map[string]uint64)
	for nodeID, pos := range poss {
		suffixs[nodeID] = 0
		if pos.Suffix > 2 {
			suffixs[nodeID] = pos.Suffix - 2
		}
	}

	lm.CommitTS = ts
	lm.Suffixs = suffixs

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(lm)
	if err != nil {
		log.Errorf("syncer save meta info to file %s err %v", lm.name, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	err = ioutil2.WriteFileAtomic(lm.name, buf.Bytes(), 0644)
	if err != nil {
		log.Errorf("syncer save meta info to file %s err %v", lm.name, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	lm.saveTime = time.Now()
	return nil
}

// Check implements Meta.Check interface.
func (lm *localMeta) Check() bool {
	lm.RLock()
	defer lm.RUnlock()

	if time.Since(lm.saveTime) >= maxSaveTime {
		return true
	}

	return false
}

// Pos implements Meta.Pos interface.
func (lm *localMeta) Pos() (int64, map[string]pb.Pos) {
	lm.RLock()
	defer lm.RUnlock()

	poss := make(map[string]pb.Pos)
	for nodeID, suffix := range lm.Suffixs {
		poss[nodeID] = pb.Pos{
			Suffix: suffix,
			Offset: 0,
		}
	}
	return lm.CommitTS, poss
}

func (lm *localMeta) String() string {
	ts, poss := lm.Pos()
	return fmt.Sprintf("binlog %s commitTS = %d positions = %+v", lm.name, ts, poss)
}
