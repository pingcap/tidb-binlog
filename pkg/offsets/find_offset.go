package offset

import (
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
)

// Seeker is a struct for finding offsets in kafka
type Seeker struct {
	topic    string
	addr     []string
	cfg      *sarama.Config
	consumer sarama.Consumer
}

const shiftBits = 18
const subTime = 20 * 60 * 1000

// NewSeeker returns Seeker instance
func NewSeeker(tp string, address []string, config *sarama.Config) (*Seeker, error) {
	err := CheckArg(tp, address)
	if err != nil {
		log.Errorf("argument is invaild")
		return nil, errors.Trace(err)
	}

	return &Seeker{
		topic: tp,
		addr:  address,
		cfg:   config,
	}, nil
}

// FindOffsetByTS gets offsets by given ts
func (sk *Seeker) FindOffsetByTS(ts int64) ([]int64, error) {
	commitTs := GetSafeTs(ts)

	partitions, err := sk.GetPartitions(sk.topic, sk.addr, sk.cfg)
	if err != nil {
		log.Errorf("get partitions error %v", err)
		return make([]int64, 0), errors.Trace(err)
	}

	return sk.GetAllOffset(partitions, commitTs)
}

// GetSafeTs return safe ts
func GetSafeTs(ts int64) int64 {
	ts = ts >> shiftBits
	ts -= subTime
	return ts
}

// GetPartitions return all partitions in one topic
func (sk *Seeker) GetPartitions(topic string, addr []string, cfg *sarama.Config) ([]int32, error) {
	consumer, err := sarama.NewConsumer(addr, cfg)
	if err != nil {
		log.Errorf("NewConsumer error %v", err)
		return make([]int32, 0), errors.Trace(err)
	}

	sk.consumer = consumer

	partitionList, err := sk.consumer.Partitions(topic)
	if err != nil {
		log.Errorf("get partitionList error %v", err)
		return make([]int32, 0), errors.Trace(err)
	}

	return partitionList, nil
}

// GetAllOffset returns all offsets in partitions
func (sk *Seeker) GetAllOffset(partitionList []int32, ts int64) ([]int64, error) {
	var offsets []int64

	for partition := range partitionList {
		var offset int64

		startOffset, err := sk.GetFirstOffset(int32(partition))
		if err != nil {
			log.Errorf("getFirstOffset error %v", err)
			return offsets, errors.Trace(err)
		}

		endOffset, err := sk.GetLastOffset(int32(partition))
		if err != nil {
			log.Errorf("getFirstOffset error %v", err)
			return offsets, errors.Trace(err)
		}

		if offset, err = sk.GetPosOffset(int32(partition), startOffset, endOffset, ts); err != nil {
			log.Errorf("getOffset error %v", err)
			return offsets, errors.Trace(err)
		}
		offsets = append(offsets, offset)
	}

	return offsets, nil
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

// GetPosOffset returns offset by given pos
func (sk *Seeker) GetPosOffset(partition int32, start int64, end int64, ts int64) (int64, error) {
	for {
		if start > end {
			break
		}

		mid := (end-start)/2 + start
		offset, err := sk.GetOneOffset(partition, mid, ts)
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
func (sk *Seeker) GetOneOffset(partition int32, offset int64, ts int64) (int64, error) {
	bg := new(pb.Binlog)

	pc, err := sk.consumer.ConsumePartition(sk.topic, partition, offset)
	if err != nil {
		log.Errorf("ConsumePartition error %v", err)
		return -1, errors.Trace(err)
	}

	defer pc.AsyncClose()

	for msg := range pc.Messages() {
		err := json.Unmarshal(msg.Value, bg)
		if err != nil {
			log.Errorf("unmarshal binlog error(%v)", err)
			return -1, errors.Trace(err)
		}

		log.Errorf("bg.CommitTs %v %v %v", bg.CommitTs, ts, offset)
		if bg.CommitTs < ts {
			return msg.Offset, nil

		}

		return -1, nil
	}

	return -1, errors.New("cannot find a valid offset")
}

// GetOffset return offset by given ts
func (sk *Seeker) GetOffset(partition int32, ts int64) (int64, error) {
	client, err := sarama.NewClient(sk.addr, sk.cfg)
	if err != nil {
		log.Errorf("create client error(%v)", err)
		return -1, errors.Trace(err)
	}

	offset, err := client.GetOffset(sk.topic, partition, ts)
	if err != nil {
		log.Errorf("get offset error(%v)", err)
		return -1, errors.Trace(err)
	}

	return offset, nil
}

// GetFirstOffset returns first offset in a partition
func (sk *Seeker) GetFirstOffset(partition int32) (int64, error) {
	return sk.GetOffset(partition, sarama.OffsetOldest)
}

// GetLastOffset returns last offset in a partition
func (sk *Seeker) GetLastOffset(partition int32) (int64, error) {
	return sk.GetOffset(partition, sarama.OffsetNewest)
}
