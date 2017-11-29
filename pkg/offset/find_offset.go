package drainer

import (
	"encoding/json"
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
)

type binlog struct {
	topic     string
	partition int32
	addr      []string
	cfg       *sarama.Config
	bg        pb.Binlog
	consumer  sarama.Consumer
}

func (bg *binlog) findOffsetByTS(ts int64) ([]int64, error) {
	commitTs := getCommitTs(ts)

	partitions, err := bg.getPartitions(bg.topic, bg.addr, bg.cfg)
	if err != nil {
		log.Errorf("get partitions error %v", err)
		return make([]int64, 0), errors.Trace(err)
	}

	return bg.getAllOffset(partitions, commitTs)
}

func getCommitTs(ts int64) int64 {
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

func (bg *binlog) getPartitions(topic string, addr []string, cfg *sarama.Config) ([]int32, error) {
	checkArg(topic, addr)

	consumer, err := sarama.NewConsumer(addr, cfg)
	if err != nil {
		log.Errorf("NewConsumer error %v", err)
		return make([]int32, 0), errors.Trace(err)
	}

	bg.consumer = consumer

	partitionList, err := bg.consumer.Partitions(topic)
	if err != nil {
		log.Errorf("get partitionList error %v", err)
		return make([]int32, 0), errors.Trace(err)
	}

	return partitionList, nil
}

func (bg *binlog) getAllOffset(partitionList []int32, ts int64) ([]int64, error) {
	var offsets []int64

	for partition := range partitionList {
		pc, err := bg.consumer.ConsumePartition(bg.topic, int32(partition), sarama.OffsetOldest)
		if err != nil {
			log.Errorf("ConsumePartition error %v", err)
			return offsets, errors.Trace(err)
		}

		defer pc.AsyncClose()
		var offset int64

		if offset, err = getOneOffset(pc, ts); err != nil {
			log.Errorf("getOffset error %v", err)
			return offsets, errors.Trace(err)
		}
		offsets = append(offsets, offset)
	}

	return offsets, nil
}

func checkArg(topic string, addr []string) {
	if topic == "" {
		topic = "Offset"
	}
	if len(addr) == 0 {
		addr = []string{"localhost:9092"}
	}
}

func getOneOffset(pc sarama.PartitionConsumer, ts int64) (int64, error) {
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
