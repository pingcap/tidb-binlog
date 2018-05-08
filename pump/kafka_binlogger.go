package pump

import (
	"strings"
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
	encoder  Encoder

	sync.RWMutex
	addr []string
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
		addr:     addr,
	}
	return binlogger, nil
}

// ReadFrom implements ReadFrom WriteTail interface
func (k *kafkaBinloger) ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error) {
	return nil, nil
}

// WriteTail implements Binlogger WriteTail interface
func (k *kafkaBinloger) WriteTail(payload []byte) (int64, error) {
	beginTime := time.Now()
	defer func() {
		writeBinlogHistogram.WithLabelValues("kafka").Observe(time.Since(beginTime).Seconds())
		writeBinlogSizeHistogram.WithLabelValues("kafka").Observe(float64(len(payload)))
		writeBinlogCounter.WithLabelValues("kafka", "success").Add(1)
	}()

	// for concurrency write
	k.RLock()
	defer k.RUnlock()

	if len(payload) == 0 {
		return 0, nil
	}

	offset, err := k.encoder.Encode(payload)
	if err != nil {
		writeBinlogCounter.WithLabelValues("kafka", "fail").Add(1)
		log.Errorf("write binlog into kafka %d error %v", latestKafkaPos.Offset, err)
	}
	if offset > latestKafkaPos.Offset {
		latestKafkaPos.Offset = offset
	}

	return offset, errors.Trace(err)
}

// Walk reads binlog from the "from" position and sends binlogs in the streaming way
func (k *kafkaBinloger) Walk(ctx context.Context, from binlog.Pos, sendBinlog func(entity binlog.Entity) error) error {
	return nil
}

// Close implements Binlogger Close interface
func (k *kafkaBinloger) Close() error {
	k.Lock()
	defer k.Unlock()

	return k.producer.Close()
}

// GC implements Binlogger GC interface
func (k *kafkaBinloger) GC(days time.Duration, pos binlog.Pos) {}

// Name implements the Binlogger interface.
func (k *kafkaBinloger) Name() string {
	return strings.Join(k.addr, ",")
}
