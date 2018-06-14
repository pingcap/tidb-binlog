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
// MetaCheckpoint keeps track of all past checkpoints. So that when FE finished a flush at CT, we'll suggest FC to save the latest checkpoint before CT.
type MetaCheckpoint struct {
	sync.Mutex
	safeCP  *checkpoint
	pastCPs []*checkpoint
}

var instance *MetaCheckpoint
var once sync.Once

// GetInstance endows singleton pattern to MetaCheckpoint.
func GetInstance() *MetaCheckpoint {
	once.Do(func() {
		instance = &MetaCheckpoint{
			safeCP:  nil,
			pastCPs: make([]*checkpoint, 0),
		}
	})
	return instance
}

// Flush picks the safe checkpoint according to flushed commit timestamp, then removes all past checkpoints until it.
func (f *MetaCheckpoint) Flush(commitTS int64) {
	f.Lock()
	defer f.Unlock()

	log.Debug("FMC received a flush, updating safe checkpoint.")

	// Find the latest cp before last flush time.
	var removeUntil = -1
	for i, cp := range f.pastCPs {
		if cp.commitTS < commitTS {
			removeUntil = i
			f.safeCP = cp
		}
	}
	// Re-make past checkpoints and discards ones until the safe checkpoint.
	if removeUntil >= 0 {
		log.Debugf("FMC picks safe checkpoint %v.", f.safeCP)
		newSafeCP := make([]*checkpoint, 0, len(f.pastCPs)-removeUntil-1)
		copy(newSafeCP, f.pastCPs[removeUntil+1:])
	} else {
		log.Debug("FMC picks no safe checkpoint.")
	}
	log.Debugf("FMC remaining %d past checkpoints.", len(f.pastCPs))
}

// PushPastCP pushes a past checkpoint.
func (f *MetaCheckpoint) PushPastCP(commitTS int64, pos map[string]pb.Pos) {
	f.Lock()
	defer f.Unlock()

	f.pastCPs = append(f.pastCPs, &checkpoint{commitTS, pos})
	log.Debugf("FMC pushed a passing checkpoint %v.", f.pastCPs[len(f.pastCPs)-1])
}

// PopSafeCP pops the safe checkpoint, after popping the safe checkpoint will be nil.
func (f *MetaCheckpoint) PopSafeCP() (bool, int64, map[string]pb.Pos) {
	f.Lock()
	defer f.Unlock()
	if f.safeCP == nil {
		log.Debug("FMC has no safe checkpoint.")
		return false, -1, nil
	}
	log.Debugf("FMC popping safe checkpoint %v.", f.safeCP)
	ok, commitTS, pos := true, f.safeCP.commitTS, f.safeCP.pos
	f.safeCP = nil
	return ok, commitTS, pos
}
