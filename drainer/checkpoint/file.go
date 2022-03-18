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
	"os"
	"sync"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"

	"github.com/pingcap/tidb-binlog/pkg/util"
)

// FileCheckPoint is local CheckPoint struct.
type FileCheckPoint struct {
	sync.RWMutex
	closed          bool
	initialCommitTS int64

	name string

	ConsistentSaved bool  `toml:"consistent" json:"consistent"`
	CommitTS        int64 `toml:"commitTS" json:"commitTS"`
	Version         int64 `toml:"schema-version" json:"schema-version"`
}

// NewFile creates a new FileCheckpoint.
func NewFile(initialCommitTS int64, filePath string) (CheckPoint, error) {
	pb := &FileCheckPoint{
		initialCommitTS: initialCommitTS,
		name:            filePath,
	}
	err := pb.Load()
	if err != nil {
		return pb, errors.Trace(err)
	}

	return pb, nil
}

// Load implements CheckPointor.Load interface.
func (sp *FileCheckPoint) Load() error {
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

	_, err = toml.NewDecoder(file).Decode(sp)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Save implements CheckPoint.Save interface
func (sp *FileCheckPoint) Save(ts, secondaryTS int64, consistent bool, version int64) error {
	sp.Lock()
	defer sp.Unlock()

	if sp.closed {
		return errors.Trace(ErrCheckPointClosed)
	}

	sp.CommitTS = ts
	sp.ConsistentSaved = consistent
	if version > sp.Version {
		sp.Version = version
	}

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(sp)
	if err != nil {
		return errors.Annotate(err, "encode checkpoint failed")
	}

	err = util.WriteFileAtomic(sp.name, buf.Bytes(), 0644)
	if err != nil {
		return errors.Annotatef(err, "write file %s failed", sp.name)
	}

	return nil
}

// TS implements CheckPoint.TS interface
func (sp *FileCheckPoint) TS() int64 {
	sp.RLock()
	defer sp.RUnlock()

	return sp.CommitTS
}

// SchemaVersion implements CheckPoint.SchemaVersion interface.
func (sp *FileCheckPoint) SchemaVersion() int64 {
	sp.RLock()
	defer sp.RUnlock()

	return sp.Version
}

// IsConsistent implements CheckPoint interface
func (sp *FileCheckPoint) IsConsistent() bool {
	sp.RLock()
	defer sp.RUnlock()

	return sp.ConsistentSaved
}

// Close implements CheckPoint.Close interface
func (sp *FileCheckPoint) Close() error {
	sp.Lock()
	defer sp.Unlock()

	if sp.closed {
		return errors.Trace(ErrCheckPointClosed)
	}

	sp.closed = true
	return nil
}
