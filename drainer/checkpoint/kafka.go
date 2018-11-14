package checkpoint

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	binlog "github.com/pingcap/tipb/go-binlog"
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
}

// SetSafeTS set the safe ts to be ts
func (m *KafkaMeta) SetSafeTS(ts int64) {
	atomic.StoreInt64(&m.safeTS, ts)
}

// GetSafeTS return the safe ts
func (m *KafkaMeta) GetSafeTS() int64 {
	return atomic.LoadInt64(&m.safeTS)
}

var kafkaMeta *KafkaMeta
var kafkaMetaOnce sync.Once

// GetKafkaMeta return the singleton instance of KafkaMeta
func GetKafkaMeta() *KafkaMeta {
	kafkaMetaOnce.Do(func() {
		kafkaMeta = new(KafkaMeta)
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

// SetSafeTS set the safe ts to be ts
func (cp *KafkaCheckpoint) SetSafeTS(ts int64) {
	cp.meta.SetSafeTS(ts)
}

// Save implements CheckPoint.Save()
func (cp *KafkaCheckpoint) Save(ts int64, pos map[string]binlog.Pos) error {
	cp.Lock()
	defer cp.Unlock()

	if ts <= cp.CommitTS {
		log.Error("ignore save ts: ", ts)
		return nil
	}

	if cp.meta.GetSafeTS() < ts {
		log.Debug("skip save ts: ", ts)
		return nil
	}

	return cp.PbCheckPoint.Save(ts, pos)
}
