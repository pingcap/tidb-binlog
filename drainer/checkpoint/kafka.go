package checkpoint

import (
	"math"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

// KafkaCheckpoint is local CheckPoint struct.
type KafkaCheckpoint struct {
	sync.Mutex
	*PbCheckPoint

	meta *KafkaMeta
}

// KafkaMeta maintains the safeTS which is synced to kafka
type KafkaMeta struct {
	// set to be the minimize commit ts of binlog if we have some binlog not having successful sent to kafka
	// or set to be `math.MaxInt64`
	safeTS int64

	lock sync.Mutex
	cond *sync.Cond
}

// SetSafeTS set the safe ts to be ts
func (m *KafkaMeta) SetSafeTS(ts int64) {
	log.Debug("set safe ts: ", ts)

	m.lock.Lock()
	m.safeTS = ts
	m.cond.Signal()
	m.lock.Unlock()
}

var kafkaMeta *KafkaMeta
var kafkaMetaOnce sync.Once

// GetKafkaMeta return the singleton instance of KafkaMeta
func GetKafkaMeta() *KafkaMeta {
	kafkaMetaOnce.Do(func() {
		kafkaMeta = new(KafkaMeta)
		kafkaMeta.cond = sync.NewCond(&kafkaMeta.lock)
	})

	return kafkaMeta
}

func newKafka(cfg *Config) (CheckPoint, error) {
	pb, err := newPb(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cp := &KafkaCheckpoint{
		PbCheckPoint: pb.(*PbCheckPoint),
		meta:         GetKafkaMeta(),
	}

	cp.meta.SetSafeTS(math.MaxInt64)

	return cp, nil
}

// Save implements CheckPoint.Save()
func (cp *KafkaCheckpoint) Save(ts int64) error {
	cp.Lock()
	defer cp.Unlock()

	if ts <= cp.CommitTS {
		log.Error("ignore save ts: ", ts)
		return nil
	}

	cp.meta.lock.Lock()
	for cp.meta.safeTS < ts {
		log.Debugf("wait for ts: %d safe_ts: %d", ts, cp.meta.safeTS)
		cp.meta.cond.Wait()
	}
	cp.meta.lock.Unlock()

	return cp.PbCheckPoint.Save(ts)
}
