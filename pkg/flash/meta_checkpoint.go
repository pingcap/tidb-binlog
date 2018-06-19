package flash

import (
	"sync"

	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/juju/errors"
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
	safeCP  *checkpoint
	pendingCPs []*checkpoint
	forceSave bool
}

var instance *MetaCheckpoint
var once sync.Once

// GetInstance endows singleton pattern to MetaCheckpoint.
func GetInstance() *MetaCheckpoint {
	once.Do(func() {
		instance = &MetaCheckpoint{
			safeCP:  nil,
			pendingCPs: make([]*checkpoint, 0),
			forceSave: false,
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
		f.safeCP = nil
	} else {
		log.Debug("FMC picks no safe checkpoint.")
	}
	log.Debugf("FMC remaining %d pending checkpoints.", len(f.pendingCPs))
}

// PushPendingCP pushes a pending checkpoint.
func (f *MetaCheckpoint) PushPendingCP(commitTS int64, pos map[string]pb.Pos) {
	f.Lock()
	defer f.Unlock()

	f.pendingCPs = append(f.pendingCPs, &checkpoint{commitTS, pos})
	log.Debugf("FMC pushed a pending checkpoint %v.", f.pendingCPs[len(f.pendingCPs)-1])
}

// PopSafeCP pops the safe checkpoint, after popping the safe checkpoint will be nil.
func (f *MetaCheckpoint) PopSafeCP() (bool, int64, map[string]pb.Pos, error) {
	f.Lock()
	defer f.Unlock()

	if f.forceSave {
		log.Debug("FMC has a force save, setting safe checkpoint to the last pushed one.")
		if len(f.pendingCPs) == 0 || f.pendingCPs[len(f.pendingCPs)-1] == nil {
			return false, -1, nil, errors.New("FMC has no pending checkpoints for force save.")
		}
		f.safeCP = f.pendingCPs[len(f.pendingCPs)-1]
		f.forceSave = false
		f.removePendingCPs(len(f.pendingCPs))
	} else if f.safeCP == nil {
		log.Debug("FMC has no safe checkpoint.")
		return false, -1, nil, nil
	}

	log.Debugf("FMC popping safe checkpoint %v.", f.safeCP)
	ok, commitTS, pos := true, f.safeCP.commitTS, f.safeCP.pos
	f.safeCP = nil
	return ok, commitTS, pos, nil
}

func (f *MetaCheckpoint) removePendingCPs(to int) {
	newPendingCP := make([]*checkpoint, 0, len(f.pendingCPs)-to)
	copy(newPendingCP, f.pendingCPs[to:])
	f.pendingCPs = newPendingCP
}
