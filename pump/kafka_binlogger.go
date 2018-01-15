package pump

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	binlog "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
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

	sync.RWMutex
}

func createKafkaBinlogger(clusterID string, node string, addr []string) (Binlogger, error) {
	producer, err := createKafkaClient(addr)
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
	k.RLock()
	defer k.RUnlock()

	if len(payload) == 0 {
		return nil
	}

	offset, err := k.encoder.encode(payload)
	if offset > latestKafkaPos.Offset {
		latestKafkaPos.Offset = offset
	}

	return errors.Trace(err)
}

// Walk reads binlog from the "from" position and sends binlogs in the streaming way
func (k *kafkaBinloger) Walk(ctx context.Context, from binlog.Pos, fSend func(entity binlog.Entity) error) (binlog.Pos, error) {
	return binlog.Pos{}, nil
}

// Close implements Binlogger Close interface
func (k *kafkaBinloger) Close() error {
	k.Lock()
	defer k.Unlock()

	return k.producer.Close()
}

// GC implements Binlogger GC interface
func (k *kafkaBinloger) GC(days time.Duration, pos binlog.Pos) {}
