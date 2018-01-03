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
	defaultOpen      = true
	defaultPartition = int32(0)
	errorClosed      = errors.New("binlogger is closed")
)

const defaultMaxBinlogItem = 1024 * 1024

type kafkaBinloger struct {
	topic string

	producer sarama.SyncProducer
	encoder  *kafkaEncoder

	closed      bool
	isAvailable bool

	sync.RWMutex
}

func createKafkaBinlogger(clusterID string, node string, addr []string) (Binlogger, error) {
	// initial kafka client to use manual partitioner
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(addr, config)
	if err != nil {
		log.Errorf("create kafka producer error: %v", err)
		return nil, errors.Trace(err)
	}

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
	k.Lock()
	defer k.Unlock()

	if len(payload) == 0 {
		return nil
	}

	offset, err := k.encoder.encode(payload)
	if offset > latestPos.Offset {
		latestPos.Offset = offset
	}
	if err != nil {
		k.closed = true
		return errors.Trace(err)
	}

	return nil
}

// IsAvailable implements Binlogger IsAvailable interface
func (k *kafkaBinloger) IsAvailable() bool {
	k.RLock()
	defer k.RUnlock()

	return !k.closed
}

// MarkAvailable implements Binlogger MarkAvailable interface
func (k *kafkaBinloger) MarkAvailable() {
	k.Lock()
	defer k.Unlock()

	k.closed = false
}

// Close implements Binlogger Close interface
func (k *kafkaBinloger) Close() error {
	k.Lock()
	defer k.Unlock()

	return k.producer.Close()
}

// GC implements Binlogger GC interface
func (k *kafkaBinloger) GC(days time.Duration) {}
