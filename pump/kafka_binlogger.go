package pump

import (
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/util"
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

	producer  sarama.SyncProducer
	encoder   *kafkaEncoder
	aproducer sarama.AsyncProducer

	sync.RWMutex
	addr []string
}

func createKafkaBinlogger(clusterID string, node string, addr []string, kafkaVersion string) (Binlogger, error) {
	producer, err := createKafkaProducer(addr, kafkaVersion)
	if err != nil {
		log.Errorf("create kafka producer error: %v", err)
		return nil, errors.Trace(err)
	}

	config, err := util.NewSaramaConfig(kafkaVersion, "pump-async-")
	if err != nil {
		return nil, errors.Trace(err)
	}

	config.Producer.MaxMessageBytes = GlobalConfig.maxMsgSize
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll

	aproducer, err := util.NewNeverFailAsyncProducer(addr, config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	topic := TopicName(clusterID, node)
	binlogger := &kafkaBinloger{
		topic:     topic,
		producer:  producer,
		aproducer: aproducer,
		encoder:   newKafkaEncoder(producer, topic, DefaultTopicPartition()),
		addr:      addr,
	}

	go binlogger.handleSuccess()

	return binlogger, nil
}

// ReadFrom implements ReadFrom WriteTail interface
func (k *kafkaBinloger) ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error) {
	return nil, nil
}

// WriteTail implements Binlogger WriteTail interface
func (k *kafkaBinloger) WriteTail(entity *binlog.Entity) (int64, error) {
	beginTime := time.Now()
	defer func() {
		writeBinlogHistogram.WithLabelValues("kafka").Observe(time.Since(beginTime).Seconds())
		writeBinlogSizeHistogram.WithLabelValues("kafka").Observe(float64(len(entity.Payload)))
	}()

	// for concurrency write
	k.RLock()
	defer k.RUnlock()

	// when have empty payload?
	if len(entity.Payload) == 0 {
		return 0, nil
	}

	offset, err := k.encoder.Encode(entity)
	if err != nil {
		writeErrorCounter.WithLabelValues("kafka").Add(1)
		log.Errorf("write binlog into kafka %d error %v", latestKafkaPos.Offset, err)
	}
	if offset > latestKafkaPos.Offset {
		latestKafkaPos.Offset = offset
	}

	return offset, errors.Trace(err)
}

type callback func(offset int64, err error)

func (k *kafkaBinloger) AsyncWriteTail(entity *binlog.Entity, cb callback) {
	log.Debugf("AsyncWriteTail pos: %v payload len: %d", entity.Pos, len(entity.Payload))
	beginTime := time.Now()
	defer func() {
		writeBinlogHistogram.WithLabelValues("kafka").Observe(time.Since(beginTime).Seconds())
		writeBinlogSizeHistogram.WithLabelValues("kafka").Observe(float64(len(entity.Payload)))
	}()

	// for concurrency write
	k.RLock()
	defer k.RUnlock()

	// when have empty payload?
	if len(entity.Payload) == 0 {
		cb(0, nil)
		return
	}

	msgs, err := k.encoder.slicer.Generate(entity)
	// should never happens
	if err != nil {
		panic(err)
	}

	for i := 0; i < len(msgs); i++ {
		// when the last msg successes, we call the callback
		if i == len(msgs)-1 {
			msgs[i].Metadata = cb
		}
		k.aproducer.Input() <- msgs[i]
	}

	return
}

func (k *kafkaBinloger) handleSuccess() {
	for msg := range k.aproducer.Successes() {
		cb, ok := msg.Metadata.(callback)
		if ok {
			offset := msg.Offset

			cb(offset, nil)
			if offset > latestKafkaPos.Offset {
				latestKafkaPos.Offset = offset
			}
		}
	}

	log.Debug("handleSuccess quit")
}

// Walk reads binlog from the "from" position and sends binlogs in the streaming way
func (k *kafkaBinloger) Walk(ctx context.Context, from binlog.Pos, sendBinlog func(entity *binlog.Entity) error) error {
	return nil
}

// Close implements Binlogger Close interface
func (k *kafkaBinloger) Close() error {
	k.Lock()
	defer k.Unlock()

	k.aproducer.AsyncClose()
	return k.producer.Close()
}

// GC implements Binlogger GC interface
func (k *kafkaBinloger) GC(days time.Duration, pos binlog.Pos) {}

// Name implements the Binlogger interface.
func (k *kafkaBinloger) Name() string {
	return strings.Join(k.addr, ",")
}
