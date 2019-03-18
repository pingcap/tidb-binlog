package checkpoint

import (
	"sync"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
)

// KafkaCheckpoint is local CheckPoint struct.
type KafkaCheckpoint struct {
	sync.Mutex
	*PbCheckPoint
}

func newKafka(cfg *Config) (CheckPoint, error) {
	pb, err := NewPb(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cp := &KafkaCheckpoint{
		PbCheckPoint: pb.(*PbCheckPoint),
	}

	return cp, nil
}

// Save implements CheckPoint.Save()
func (cp *KafkaCheckpoint) Save(ts int64) error {
	cp.Lock()
	defer cp.Unlock()

	if cp.closed {
		return errors.Trace(ErrCheckPointClosed)
	}

	if ts <= cp.CommitTS {
		log.Error("ignore save ts: ", ts)
		return nil
	}

	return cp.PbCheckPoint.Save(ts)
}
