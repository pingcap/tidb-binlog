package offsets

import (
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"golang.org/x/net/context"
)

const (
	less  = -1
	equal = 0
	large = 1
)

// Seeker is a struct for finding offsets in kafka/rocketmq
type Seeker interface {
	Do(ctx context.Context, topic string, pos interface{}, startTime int64, endTime int64, partitions []int32) ([]int64, error)
	Close() error
}

// Operator is an interface for seeker operation
type Operator interface {
	// Decode decodes message slices from kafka or rocketmq
	// return message's position
	Decode(ctx context.Context, messages <-chan *sarama.ConsumerMessage) (interface{}, error)
	// Compare compares excepted and current position, return
	// -1 if exceptedPos < currentPos
	// 0 if exceptedPos == currentPos
	// 1 if exceptedPos > currentPos
	Compare(exceptedPos interface{}, currentPos interface{}) (int, error)
}

// KafkaSeeker implements Kafka Seeker
type KafkaSeeker struct {
	addr     []string
	cfg      *sarama.Config
	consumer sarama.Consumer
	client   sarama.Client

	operator Operator
}

// NewKafkaSeeker returns Seeker instance
func NewKafkaSeeker(address []string, config *sarama.Config, operator Operator) (Seeker, error) {
	if address == nil {
		return nil, errors.New("address is nil")
	}

	consumer, err := sarama.NewConsumer(address, config)
	if err != nil {
		log.Errorf("NewConsumer error %v", err)
		return nil, errors.Trace(err)
	}

	client, err := sarama.NewClient(address, config)
	if err != nil {
		log.Errorf("create client error(%v)", err)
		return nil, errors.Trace(err)
	}

	return &KafkaSeeker{
		addr:     address,
		cfg:      config,
		consumer: consumer,
		operator: operator,
		client:   client,
	}, nil
}

// Close shuts down consumer and client of Kafka
func (ks *KafkaSeeker) Close() error {
	ks.consumer.Close()
	ks.client.Close()
	return nil
}

// Do returns offsets by given pos
func (ks *KafkaSeeker) Do(ctx context.Context, topic string, pos interface{}, startTime int64, endTime int64, partitions []int32) ([]int64, error) {
	var err error
	if len(partitions) == 0 {
		partitions, err = ks.consumer.Partitions(topic)
		if err != nil {
			log.Errorf("get partitions from topic %s error %v", topic, err)
			return nil, errors.Annotatef(err, "get partitions from topic %s", topic)
		}
	}

	offsets, err := ks.seekOffsets(ctx, topic, partitions, pos)
	if err != nil {
		log.Errorf("seek offsets error %v", err)
	}
	return offsets, errors.Trace(err)
}

// seekOffsets returns all valid offsets in partitions
func (ks *KafkaSeeker) seekOffsets(ctx context.Context, topic string, partitions []int32, pos interface{}) ([]int64, error) {
	offsets := make([]int64, len(partitions))
	for _, partition := range partitions {
		start, err := ks.getOffset(topic, partition, sarama.OffsetOldest)
		if err != nil {
			return offsets, errors.Annotatef(err, "get oldest offset from topic %s partition %d", topic, partition)
		}

		end, err := ks.getOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			return offsets, errors.Annotatef(err, "get newest offset from topic %s partition %d", topic, partition)
		}

		log.Infof("seek position %v in topic %s partition %d, oldest offset %d, newest offset %d", pos, topic, partition, start, end)
		offset, err := ks.seekOffset(ctx, topic, partition, start, end-1, pos)
		if err != nil {
			return offsets, errors.Annotatef(err, "seek Offset in topic %s partition %d", topic, partition)
		}
		log.Infof("seek position %v in topic %s partition %d, offset %d", pos, topic, partition, offset)
		offsets[partition] = offset
	}

	return offsets, nil
}

func (ks *KafkaSeeker) seekOffset(ctx context.Context, topic string, partition int32, start int64, end int64, pos interface{}) (int64, error) {
	cmp, startPos, firstOffset, err := ks.getAndCompare(ctx, topic, partition, start, pos)
	if err != nil {
		return -1, errors.Trace(err)
	}
	if cmp == -1 {
		log.Errorf("given position %v is smaller than oldest message's position %v, some binlogs may lose", pos, startPos)
	}
	if cmp <= 0 {
		return firstOffset, nil
	}

	for start < end-1 {
		mid := (end-start)/2 + start
		cmp, _, firstOffset, err = ks.getAndCompare(ctx, topic, partition, mid, pos)
		if err != nil {
			return -1, errors.Trace(err)
		}

		switch cmp {
		case less:
			end = mid - 1
		case equal:
			return firstOffset, nil
		case large:
			start = mid
		}

	}

	cmp, _, _, err = ks.getAndCompare(ctx, topic, partition, end, pos)
	if err != nil {
		return -1, errors.Trace(err)
	}
	if cmp >= equal {
		return end, nil
	}

	return start, nil
}

// getAndCompare queries message at give offset and compare pos with it's position
// returns Opeator.Compare()
func (ks *KafkaSeeker) getAndCompare(ctx context.Context, topic string, partition int32, offset int64, pos interface{}) (int, interface{}, int64, error) {
	pc, err := ks.consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		log.Errorf("ConsumePartition error %v", err)
		return 0, nil, offset, errors.Trace(err)
	}
	defer pc.Close()

	bp, err := ks.operator.Decode(ctx, pc.Messages())
	if err != nil {
		return 0, bp, offset, errors.Annotate(err, "decode message")
	}
	cmp, err := ks.operator.Compare(pos, bp)
	if err != nil {
		return 0, bp, offset, errors.Annotatef(err, "compare %s with position %v", bp, pos)
	}
	return cmp, bp, offset, nil
}

// getOffset return offset by given pos
func (ks *KafkaSeeker) getOffset(topic string, partition int32, pos int64) (int64, error) {
	offset, err := ks.client.GetOffset(topic, partition, pos)
	if err != nil {
		return -1, errors.Trace(err)
	}

	return offset, nil
}
