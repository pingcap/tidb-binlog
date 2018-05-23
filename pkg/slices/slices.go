package slices

import (
	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/bitmap"
	"github.com/pingcap/tidb-binlog/pump"
)

// Tracker is a struct for tracking slices of a binlog in kafka/rocketmq
type Tracker interface {
	// Slices gets all slices of a binlog for specified topic and partition with >= offset
	Slices(topic string, partition int32, offset int64) ([]interface{}, error)
	Close() error
}

// KafkaTracker implements Kafka Tracker
type KafkaTracker struct {
	addr     []string
	cfg      *sarama.Config
	consumer sarama.Consumer
}

// NewKafkaTracker returns Tracker instance
func NewKafkaTracker(address []string, config *sarama.Config) (Tracker, error) {
	if address == nil {
		return nil, errors.New("address is nil")
	}

	consumer, err := sarama.NewConsumer(address, config)
	if err != nil {
		log.Errorf("NewConsumer error %v", err)
		return nil, errors.Trace(err)
	}
	return &KafkaTracker{
		addr:     address,
		cfg:      config,
		consumer: consumer,
	}, nil
}

// Close shuts down consumer of Kafka
func (t *KafkaTracker) Close() error {
	t.consumer.Close()
	return nil
}

// Slices returns all slices of a binlog
func (t *KafkaTracker) Slices(topic string, partition int32, offset int64) ([]interface{}, error) {
	cp, err := t.consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		log.Errorf("ConsumePartition error %v", err)
		return nil, errors.Trace(err)
	}
	defer cp.Close()

	msg := <-cp.Messages()
	// unsplit binlog
	if len(msg.Headers) == 0 {
		return []interface{}{msg}, nil
	}
	bms := make(map[string]*bitmap.Bitmap)
	sos := make(map[string][]int64) // cache offset rather than ConsumerMessage
	messageID := ""
	for {
		messageID = string(GetValueFromComsumerMessageHeader(pump.MessageID, msg))
		bm, ok := bms[messageID]
		so, ok := sos[messageID]
		if !ok {
			totalByte := GetValueFromComsumerMessageHeader(pump.Total, msg)
			total := int(binary.LittleEndian.Uint32(totalByte))
			bm = bitmap.NewBitmap(total)
			bms[messageID] = bm
			so = make([]int64, total)
			sos[messageID] = so
		}
		noByte := GetValueFromComsumerMessageHeader(pump.No, msg)
		no := int(binary.LittleEndian.Uint32(noByte))
		isNew := bm.Set(no)
		if isNew {
			so[no] = msg.Offset
			if bm.Completed() {
				break
			}
		}
		msg = <-cp.Messages()
	}

	so := sos[messageID]
	slices := make([]interface{}, len(so))
	for i, offset := range so {
		cp, err = t.consumer.ConsumePartition(topic, partition, offset)
		if err != nil {
			log.Errorf("ConsumePartition error %v", err)
			return nil, errors.Trace(err)
		}
		slices[i] = <-cp.Messages()
	}
	return slices, nil
}

// GetValueFromComsumerMessageHeader gets value from message header
func GetValueFromComsumerMessageHeader(key []byte, message *sarama.ConsumerMessage) []byte {
	for _, record := range message.Headers {
		if string(record.Key) == string(key) {
			return record.Value
		}
	}
	return nil
}
