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

package flash

import (
	"sync"

	"github.com/ngaut/log"
)

type checkpoint struct {
	commitTS int64
}

// MetaCheckpoint is used to connect flash executor (FE) and flash checkpoint (FC) to come to an agreement about the safe point to save the checkpoint.
// FE writes rows kind of asynchronously. That means at some point that FC thinks it's saved, FE may have not written the rows yet.
// MetaCheckpoint keeps track of all pending checkpoints. So that when FE finished a flush at CT, we'll suggest FC to save the latest checkpoint before CT.
type MetaCheckpoint struct {
	sync.Mutex
	safeCP     *checkpoint
	pendingCPs []*checkpoint
	forceSave  bool
}

var instance *MetaCheckpoint
var once sync.Once

// GetInstance endows singleton pattern to MetaCheckpoint.
func GetInstance() *MetaCheckpoint {
	once.Do(func() {
		instance = &MetaCheckpoint{
			safeCP:     nil,
			pendingCPs: make([]*checkpoint, 0),
			forceSave:  false,
		}
	})
	return instance
}

// Flush picks the safe checkpoint according to flushed commit timestamp, then removes all pending checkpoints until it.
func (f *MetaCheckpoint) Flush(commitTS int64, forceSave bool) {
	f.Lock()
	defer f.Unlock()

	if forceSave {
		log.Debugf("[flush] FMC received a force save at %d.", commitTS)
		f.forceSave = forceSave
		return
	} else if f.forceSave {
		// Now being called from flushing thread, ignore as we are about to force save right away.
		log.Debugf("[flush] FMC received a flush during force save period at %d. Ignoring.", commitTS)
		return
	}

	log.Debug("[flush] FMC received a flush, updating safe checkpoint.")

	// Find the latest cp before last flush time.
	var removeUntil = -1
	for i, cp := range f.pendingCPs {
		if cp.commitTS < commitTS {
			removeUntil = i
			f.safeCP = cp
		}
	}
	// Re-make pending checkpoints and discards ones until the safe checkpoint.
	if removeUntil >= 0 {
		log.Debugf("[flush] FMC picks safe checkpoint %v.", f.safeCP)
		f.removePendingCPs(removeUntil + 1)
	} else {
		log.Debug("[flush] FMC picks no safe checkpoint.")
	}

	log.Debugf("[flush] FMC remaining %d pending checkpoints.", len(f.pendingCPs))
}

// PushPendingCP pushes a pending checkpoint.
func (f *MetaCheckpoint) PushPendingCP(commitTS int64) {
	f.Lock()
	defer f.Unlock()

	f.pendingCPs = append(f.pendingCPs, &checkpoint{commitTS})

	log.Debugf("[push] FMC pushed a pending checkpoint %v.", f.pendingCPs[len(f.pendingCPs)-1])
}

// PopSafeCP pops the safe checkpoint, after popping the safe checkpoint will be nil.
// Returns forceSave, ok, commitTS, pos
func (f *MetaCheckpoint) PopSafeCP() (bool, bool, int64) {
	f.Lock()
	defer f.Unlock()

	if f.forceSave {
		log.Debug("[pop] FMC has a force save, clearing all.")
		f.safeCP = nil
		f.removePendingCPs(len(f.pendingCPs))
		f.forceSave = false
		return true, false, -1
	} else if f.safeCP == nil {
		log.Debug("[pop] FMC has no safe checkpoint.")
		return false, false, -1
	}

	log.Debugf("[pop] FMC popping safe checkpoint %v.", f.safeCP)

	commitTS := f.safeCP.commitTS
	f.safeCP = nil
	return false, true, commitTS
}

func (f *MetaCheckpoint) removePendingCPs(to int) {
	newPendingCP := make([]*checkpoint, 0, len(f.pendingCPs)-to)
	newPendingCP = append(newPendingCP, f.pendingCPs[to:]...)
	f.pendingCPs = newPendingCP
}
