// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package checkpoint

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/ioutil2"
)

var (
	maxSaveTime = 3 * time.Second
)

// PbCheckPoint is local CheckPoint struct.
type PbCheckPoint struct {
	sync.RWMutex
	closed          bool
	initialCommitTS int64

	name     string
	saveTime time.Time

	CommitTS int64 `toml:"commitTS" json:"commitTS"`
}

// NewPb creates a new Pb.
func newPb(cfg *Config) (CheckPoint, error) {
	pb := &PbCheckPoint{initialCommitTS: cfg.InitialCommitTS, name: cfg.CheckPointFile, saveTime: time.Now()}
	err := pb.Load()
	if err != nil {
		return pb, errors.Trace(err)
	}

	return pb, nil
}

// Load implements CheckPointor.Load interface.
func (sp *PbCheckPoint) Load() error {
	sp.Lock()
	defer sp.Unlock()

	if sp.closed {
		return errors.Trace(ErrCheckPointClosed)
	}

	defer func() {
		if sp.CommitTS == 0 {
			sp.CommitTS = sp.initialCommitTS
		}
	}()

	file, err := os.Open(sp.name)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return errors.Trace(err)
	}
	if os.IsNotExist(errors.Cause(err)) {
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
func (sp *PbCheckPoint) Save(ts int64) error {
	sp.Lock()
	defer sp.Unlock()

	if sp.closed {
		return errors.Trace(ErrCheckPointClosed)
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
func (sp *PbCheckPoint) Check(int64) bool {
	sp.RLock()
	defer sp.RUnlock()

	if sp.closed {
		return false
	}

	return time.Since(sp.saveTime) >= maxSaveTime
}

// TS implements CheckPoint.TS interface
func (sp *PbCheckPoint) TS() int64 {
	sp.RLock()
	defer sp.RUnlock()

	return sp.CommitTS
}

func (sp *PbCheckPoint) Close() error {
	sp.Lock()
	defer sp.Unlock()

	if sp.closed {
		return errors.Trace(ErrCheckPointClosed)
	}

	sp.closed = true
	return nil
}

// String implements CheckPoint.String interface
func (sp *PbCheckPoint) String() string {
	ts := sp.TS()
	return fmt.Sprintf("binlog commitTS = %d", ts)
}
