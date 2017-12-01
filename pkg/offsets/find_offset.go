package offsets

import (
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
)

const shiftBits = 18
const subTime = 20 * 60 * 1000

// Seeker is a struct for finding offsets in kafka/rocketmq
type Seeker interface {
	FindOffset(pos interface{}) ([]int64, error)
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
	topic    string
	addr     []string
	cfg      *sarama.Config
	consumer sarama.Consumer

	operator Operator
}

// NewKafkaSeeker returns Seeker instance
func NewKafkaSeeker(tp string, address []string, config *sarama.Config, operator Operator) (Seeker, error) {
	err := CheckArg(tp, address)
	if err != nil {
		log.Errorf("argument is invaild")
		return nil, errors.Trace(err)
	}

	consumer, err := sarama.NewConsumer(address, config)
	if err != nil {
		log.Errorf("NewConsumer error %v", err)
		return nil, errors.Trace(err)
	}
	
	return &KafkaSeeker{
		topic: tp,
		addr:  address,
		cfg:   config,
		consumer: consumer,
		operator: operator,
	}, nil
}

// FindOffsetByTS gets offsets by given ts
func (ks *KafkaSeeker) FindOffset(pos interface{}) ([]int64, error) {
	partitions, err := ks.GetPartitions(ks.addr, ks.cfg)
	if err != nil {
		log.Errorf("get partitions error %v", err)
		return make([]int64, 0), errors.Trace(err)
	}

	return ks.GetAllOffset(partitions, ks.operator, pos)
}

// GetPartitions return all partitions in one topic
func (ks *KafkaSeeker) GetPartitions(addr []string, cfg *sarama.Config) ([]int32, error) {
	partitionList, err := ks.consumer.Partitions(ks.topic)
	if err != nil {
		log.Errorf("get partitionList error %v", err)
		return make([]int32, 0), errors.Trace(err)
	}

	return partitionList, nil
}

// GetAllOffset returns all offsets in partitions
func (ks *KafkaSeeker) GetAllOffset(partitionList []int32, operator Operator, pos interface{}) ([]int64, error) {
	var offsets []int64

	for partition := range partitionList {
		var offset int64

		startOffset, err := ks.GetFirstOffset(int32(partition))
		if err != nil {
			log.Errorf("getFirstOffset error %v", err)
			return offsets, errors.Trace(err)
		}

		endOffset, err := ks.GetLastOffset(int32(partition))
		if err != nil {
			log.Errorf("getFirstOffset error %v", err)
			return offsets, errors.Trace(err)
		}

		if offset, err = ks.GetPosOffset(int32(partition), startOffset, endOffset, pos); err != nil {
			log.Errorf("getOffset error %v", err)
			return offsets, errors.Trace(err)
		}
		offsets = append(offsets, offset)
	}

	return offsets, nil
}

// GetPosOffset returns offset by given pos
func (ks *KafkaSeeker) GetPosOffset(partition int32, start int64, end int64, pos interface{}) (int64, error) {
	res, err := ks.operator.Compare(pos, end)
	if err != nil {
		return -1, errors.Trace(err)
	}

	if res > 0 {
		return -1, errors.New("cannot get invalid offset")
	}

	for {
		if start > end {
			break
		}

		mid := (end - start) / 2 + start
		offset, err := ks.GetOneOffset(partition, mid, pos)
		if err != nil {
			log.Errorf("get offset error %v", err)
			return -1, errors.Trace(err)
		}
		if offset != int64(-1) {
			return offset, nil
		}

		end = mid - 1
	}

	return -1, errors.New("cannot get invalid offset")
}

// GetOneOffset returns one offset
func (ks *KafkaSeeker) GetOneOffset(partition int32, offset int64, pos interface{}) (int64, error) {

	pc, err := ks.consumer.ConsumePartition(ks.topic, partition, offset)
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
		log.Infof("bg.CommitTs %v %v %v", bg.CommitTs, pos, offset)
		res, err := ks.operator.Compare(pos, bg.CommitTs) 
		if err != nil {
			log.Errorf("Compare error %v", err)
			return -1, errors.Trace(err)
		}

		if res > 0 {
			return msg.Offset, nil

		}

		return -1, nil
	}

	return -1, errors.New("cannot find a valid offset")
}

// GetOffset return offset by given pos
func (ks *KafkaSeeker) GetOffset(partition int32,  pos int64) (int64, error) {
	client, err := sarama.NewClient(ks.addr, ks.cfg)
	if err != nil {
		log.Errorf("create client error(%v)", err)
		return -1, errors.Trace(err)
	}

	offset, err := client.GetOffset(ks.topic, partition, pos)
	if err != nil {
		log.Errorf("get offset error(%v)", err)
		return -1, errors.Trace(err)
	}

	return offset, nil
}

// CheckArg checks argument
func CheckArg(topic string, addr []string) error {
	if topic == "" {
		log.Errorf("Topic is nil")
		return errors.New("Kafka topic is error")
	}
	if len(addr) == 0 {
		log.Errorf("Addr is nil")
		return errors.New("Kafka addr is nil")
	}

	return nil
}

// GetFirstOffset returns first offset in a partition
func (ks *KafkaSeeker) GetFirstOffset(partition int32) (int64, error) {
	return ks.GetOffset(partition, sarama.OffsetOldest)
}

// GetLastOffset returns last offset in a partition
func (ks *KafkaSeeker) GetLastOffset(partition int32) (int64, error) {
	return ks.GetOffset(partition, sarama.OffsetNewest)
}
