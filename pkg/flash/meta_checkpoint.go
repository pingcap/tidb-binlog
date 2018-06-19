package flash

import (
	"sync"

	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
)

type checkpoint struct {
	commitTS int64
	pos      map[string]pb.Pos
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
		log.Debugf("FMC received a force save at %d.", commitTS)
		f.forceSave = forceSave
		return
	} else if f.forceSave {
		// Now being called from flushing thread, ignore as we are about to force save right away.
		log.Debugf("FMC received a flush during force save period at %d. Ignoring.", commitTS)
		return
	}

	log.Debug("FMC received a flush, updating safe checkpoint.")

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
		log.Debugf("FMC picks safe checkpoint %v.", f.safeCP)
		f.removePendingCPs(removeUntil + 1)
	} else {
		log.Debug("FMC picks no safe checkpoint.")
	}

	log.Debugf("FMC remaining %d pending checkpoints.", len(f.pendingCPs))
}

// PushPendingCP pushes a pending checkpoint.
func (f *MetaCheckpoint) PushPendingCP(commitTS int64, pos map[string]pb.Pos) {
	f.Lock()
	defer f.Unlock()

	// Deep copy the pos.
	newPos := make(map[string]pb.Pos)
	for node, poss := range pos {
		newPos[node] = pb.Pos{poss.Suffix, poss.Offset}
	}
	f.pendingCPs = append(f.pendingCPs, &checkpoint{commitTS, newPos})

	log.Debugf("FMC pushed a pending checkpoint %v.", f.pendingCPs[len(f.pendingCPs)-1])
}

// PopSafeCP pops the safe checkpoint, after popping the safe checkpoint will be nil.
// Returns forceSave, ok, commitTS, pos
func (f *MetaCheckpoint) PopSafeCP() (bool, bool, int64, map[string]pb.Pos) {
	f.Lock()
	defer f.Unlock()

	if f.forceSave {
		log.Debug("FMC has a force save, clearing all.")
		f.safeCP = nil
		f.removePendingCPs(len(f.pendingCPs))
		f.forceSave = false
		return true, false, -1, nil
	} else if f.safeCP == nil {
		log.Debug("FMC has no safe checkpoint.")
		return false, false, -1, nil
	}

	log.Debugf("FMC popping safe checkpoint %v.", f.safeCP)

	commitTS, pos := f.safeCP.commitTS, f.safeCP.pos
	f.safeCP = nil
	return false, true, commitTS, pos
}

func (f *MetaCheckpoint) removePendingCPs(to int) {
	newPendingCP := make([]*checkpoint, 0, len(f.pendingCPs)-to)
	copy(newPendingCP, f.pendingCPs[to:])
	f.pendingCPs = newPendingCP
}
