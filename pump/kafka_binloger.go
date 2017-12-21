package pump

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	queue "github.com/golang-collections/collections/queue"
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

	qu            *queue.Queue
	maxBinlogItem int

	closed      bool
	isAvailable bool
	isOpen      bool

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
		topic:         topic,
		producer:      producer,
		encoder:       newKafkaEncoder(producer, topic, DefaultTopicPartition()),
		maxBinlogItem: defaultMaxBinlogItem,
		qu:            queue.New(),
		isOpen:        defaultOpen,
		isAvailable:   true,
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

	if !k.isAvailable {
		return k.handleFail(payload)
	}

	offset, err := k.encoder.encode(payload)
	if offset > latestPos.Offset {
		latestPos.Offset = offset
	}

	if err != nil && k.isOpen {
		k.isAvailable = false
		log.Warningf("kafka is not available now")
		kafkaFailCounter.Add(float64(1))
		return k.handleFail(payload)
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

func (k *kafkaBinloger) handleFail(payload []byte) error {

	binlogCounter.Add(float64(1))
	k.qu.Enqueue(payload)
	if k.qu.Len() == k.maxBinlogItem {
		log.Warningf("binlog number %d is too large in memory", k.qu.Len())
		k.qu.Dequeue()
	}

	select {
	case <-time.After(genBinlogInterval):
		k.writeTailSeq()
	}

	return nil
}

func (k *kafkaBinloger) writeTailSeq() {
	if k.qu.Len() == 0 {
		return
	}
	payload := k.qu.Peek()

	_, err := k.encoder.encode(payload.([]byte))
	if err == nil {
		k.qu.Dequeue()

		for k.qu.Len() > 0 {
			payload = k.qu.Dequeue()
			k.encoder.encode(payload.([]byte))
		}
		k.isAvailable = true
	}
}
