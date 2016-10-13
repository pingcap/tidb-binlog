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
	Save(pos int64) error

	// Pos gets position information.
	Pos() int64
}

// LocalMeta is local meta struct.
type localMeta struct {
	sync.RWMutex

	name     string
	saveTime time.Time

	BinLogPos int64 `toml:"binlog-pos" json:"binlog-pos"`
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
func (lm *localMeta) Save(pos int64) error {
	lm.Lock()
	defer lm.Unlock()

	lm.BinLogPos = pos

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

// Pos implements Meta.Pos interface.
func (lm *localMeta) Pos() int64 {
	lm.RLock()
	defer lm.RUnlock()

	return lm.BinLogPos
}

func (lm *localMeta) String() string {
	return fmt.Sprintf("binlog %s pos = %d", lm.name, lm.Pos())
}
