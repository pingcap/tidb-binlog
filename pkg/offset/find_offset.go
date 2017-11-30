package offset

import (
	"encoding/json"
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
)

type OffsetSeeker struct {
	topic     string
	partition int32
	addr      []string
	cfg       *sarama.Config
	consumer  sarama.Consumer
}

func (sk *OffsetSeeker) FindOffsetByTS(ts int64) ([]int64, error) {
	commitTs := GetCommitTs(ts)

	partitions, err := sk.GetPartitions(sk.topic, sk.addr, sk.cfg)
	if err != nil {
		log.Errorf("get partitions error %v", err)
		return make([]int64, 0), errors.Trace(err)
	}

	return sk.GetAllOffset(partitions, commitTs)
}

func GetCommitTs(ts int64) int64 {
	tm := time.Unix(ts, 0)
	minute := tm.Minute()
	hour := tm.Hour()

	if minute > 20 {
		minute -= 20
	} else {
		hour--
		minute += 40
	}

	tm = time.Date(tm.Year(), tm.Month(), tm.Day(), hour, minute, tm.Second(), tm.Nanosecond(), time.UTC)
	return tm.Unix()
}

func (sk *OffsetSeeker) GetPartitions(topic string, addr []string, cfg *sarama.Config) ([]int32, error) {
	err := checkArg(topic, addr)
        if err != nil {
                log.Errorf("argument is invaild")
                return make([]int32, 0), err
        }

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

func (sk *OffsetSeeker) GetAllOffset(partitionList []int32, ts int64) ([]int64, error) {
	var offsets []int64

	for partition := range partitionList {
		pc, err := sk.consumer.ConsumePartition(sk.topic, int32(partition), sarama.OffsetOldest)
		if err != nil {
			log.Errorf("ConsumePartition error %v", err)
			return offsets, errors.Trace(err)
		}

		defer pc.AsyncClose()
		var offset int64

		if offset, err = GetOneOffset(pc, ts); err != nil {
			log.Errorf("getOffset error %v", err)
			return offsets, errors.Trace(err)
		}
		offsets = append(offsets, offset)
	}

	return offsets, nil
}

func checkArg(topic string, addr []string) error{
	if topic == "" {
		log.Errorf("Topic is nil")
                return error.New("Kafka topic is error")
	}
	if len(addr) == 0 {
		log.Errorf("Addr is nil")
                return error.New("Kafka addr is nil")
	}
}

func GetOneOffset(pc sarama.PartitionConsumer, ts int64) (int64, error) {
	bg := new(pb.Binlog)

	for msg := range pc.Messages() {
		err := json.Unmarshal(msg.Value, bg)
		if err != nil {
			log.Errorf("unmarshal binlog error(%v)", err)
			return -1, errors.Trace(err)
		}

		if bg.CommitTs <= ts {
			return msg.Offset, nil
		}
	}

	return -1, nil
}
