package pump

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	binlog "github.com/pingcap/tipb/go-binlog"
)

var (
	defualtPartition = int32(0)
	errorClosed      = errors.New("binlogger is closed")
)

type kafkaBinloger struct {
	topic string

	producer sarama.SyncProducer
	encoder  *kafkaEncoder

	closed bool
	sync.RWMutex
}

func createKafkaBinlogger(clusterID string, node string, addr []string) (Binlogger, error) {
	producer, err := sarama.NewSyncProducer(addr, nil)

	if err != nil {
		log.Errorf("create kafka producer error: %v, kafka addr: %s", err, addr)
		return nil, errors.Trace(err)
	}
	log.Infof("create kafka producer success, kafka addr: %s", addr)

	topic := TopicName(clusterID, node)
	binlogger := &kafkaBinloger{
		topic:    topic,
		producer: producer,
		encoder:  newKafkaEncoder(producer, topic, DefaultTopicPartition()),
	}
	return binlogger, nil
}

// ReadFrom implements ReadFrom WriteTail interface
func (k *kafkaBinloger) ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error) {
	return nil, nil
}

// WriteTail implements Binlogger WriteTail interface
func (k *kafkaBinloger) WriteTail(payload []byte) error {
	// for concurrency write
	k.RLock()
	defer k.RUnlock()

	if k.isClosed() {
		return errorClosed
	}

	if len(payload) == 0 {
		return nil
	}

	offset, err := k.encoder.encode(payload)
	if offset > latestPos.Offset {
		latestPos.Offset = offset
	}
	return errors.Trace(err)
}

// Close implements Binlogger Close interface
func (k *kafkaBinloger) Close() error {
	k.Lock()
	defer k.Unlock()

	k.closed = true
	return k.producer.Close()
}

// GC implements Binlogger GC interface
func (k *kafkaBinloger) GC(days time.Duration) {}

func (k *kafkaBinloger) isClosed() bool {
	return k.closed == true
}
