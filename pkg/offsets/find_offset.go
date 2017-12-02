package offsets

import (
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
)

// Seeker is a struct for finding offsets in kafka/rocketmq
type Seeker interface {
	Do(topic string, pos interface{}, startTime int64, endTime int64) ([]int64, error)
}

// Operator is an interface for seeker operation
type Operator interface {
	// Decode decodes message from kafka or rocketmq
	Decode(message *sarama.ConsumerMessage) (interface{}, error)
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

// Do gets offsets by given pos
func (ks *KafkaSeeker) Do(topic string, pos interface{}, startTime int64, endTime int64) ([]int64, error) {
	partitions, err := ks.getPartitions(topic, ks.addr, ks.cfg)
	if err != nil {
		log.Errorf("get partitions error %v", err)
		return make([]int64, 0), errors.Trace(err)
	}

	return ks.getAllOffset(topic, partitions, pos)
}

// getPartitions return all partitions in one topic
func (ks *KafkaSeeker) getPartitions(topic string, addr []string, cfg *sarama.Config) ([]int32, error) {
	partitionList, err := ks.consumer.Partitions(topic)
	if err != nil {
		log.Errorf("get partitionList error %v", err)
		return nil, errors.Trace(err)
	}

	return partitionList, nil
}

// getAllOffset returns all offsets in partitions
func (ks *KafkaSeeker) getAllOffset(topic string, partitionList []int32, pos interface{}) ([]int64, error) {
	var offsets []int64

	for partition := range partitionList {
		var offset int64

		startOffset, err := ks.getFirstOffset(topic, int32(partition))
		if err != nil {
			log.Errorf("getFirstOffset error %v", err)
			return offsets, errors.Trace(err)
		}

		endOffset, err := ks.getLastOffset(topic, int32(partition))
		if err != nil {
			log.Errorf("getFirstOffset error %v", err)
			return offsets, errors.Trace(err)
		}

		if offset, err = ks.getPosOffset(topic, int32(partition), startOffset, endOffset-1, pos); err != nil {
			log.Errorf("getOffset error %v", err)
			return offsets, errors.Trace(err)
		}
		offsets = append(offsets, offset)
	}

	return offsets, nil
}

// getPosOffset returns offset by given pos
func (ks *KafkaSeeker) getPosOffset(topic string, partition int32, start int64, end int64, pos interface{}) (int64, error) {
	for {
		if start >= end {
			break
		}

		tag, err := ks.tsLessPos(topic, partition, end, pos)
		if err != nil {
			return -1, errors.Trace(err)
		}
		if tag {
			return end, nil
		}

		mid := (end-start)/2 + start
		offset, err := ks.getOneOffset(topic, partition, mid, pos)
		if err != nil {
			log.Errorf("get offset error %v", err)
			return -1, errors.Trace(err)
		}

		if offset != int64(-1) {
			start = mid + 1
		} else {
			end = mid - 1
		}
	}

	tag, err := ks.tsLessPos(topic, partition, start, pos)
	if err == nil && tag {
		return start, nil
	}

	return -1, errors.New("cannot get a valid offset")
}

// tsLessPos returns ture if binlog.ts <= pos otherwise returns false
func (ks *KafkaSeeker) tsLessPos(topic string, partition int32, offset int64, pos interface{}) (bool, error) {
	res, err := ks.getOneOffset(topic, partition, offset, pos)
	if err != nil {
		return false, errors.New("get end offset error")
	}

	if res != int64(-1) {
		return true, nil
	}

	return false, nil
}

// getOneOffset returns one offset
func (ks *KafkaSeeker) getOneOffset(topic string, partition int32, offset int64, pos interface{}) (int64, error) {
	pc, err := ks.consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		log.Errorf("ConsumePartition error %v", err)
		return -1, errors.Trace(err)
	}

	defer pc.AsyncClose()

	for msg := range pc.Messages() {
		result, err := ks.operator.Decode(msg)
		if err != nil {
			log.Errorf("unmarshal binlog error(%v)", err)
			return -1, errors.Trace(err)
		}

		bg := result.(*pb.Binlog)
		res, err := ks.operator.Compare(pos, bg.CommitTs)
		if err != nil {
			log.Errorf("Compare error %v", err)
			return -1, errors.Trace(err)
		}

		if res >= 0 {
			return msg.Offset, nil
		}

		return -1, nil
	}

	return -1, errors.New("cannot find a valid offset")
}

// getOffset return offset by given pos
func (ks *KafkaSeeker) getOffset(topic string, partition int32, pos int64) (int64, error) {
	offset, err := ks.client.GetOffset(topic, partition, pos)
	if err != nil {
		log.Errorf("get offset error(%v)", err)
		return -1, errors.Trace(err)
	}

	return offset, nil
}

// getFirstOffset returns kafka first offset
func (ks *KafkaSeeker) getFirstOffset(topic string, partition int32) (int64, error) {
	return ks.getOffset(topic, partition, sarama.OffsetOldest)
}

// getLastOffset returns kafka last offset
func (ks *KafkaSeeker) getLastOffset(topic string, partition int32) (int64, error) {
	return ks.getOffset(topic, partition, sarama.OffsetNewest)
}
