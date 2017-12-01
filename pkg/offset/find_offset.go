package offset

import (
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
)

type OffsetSeeker struct {
	topic     string
	addr      []string
	cfg       *sarama.Config
	consumer  sarama.Consumer
}

const shiftBits = 18
const subTime = 20 * 60 * 1000

// FindOffsetByTS implements offset.FindOffsetByTs
func (sk *OffsetSeeker) FindOffsetByTS(ts int64) ([]int64, error) {
	commitTs := GetSafeTs(ts)

	partitions, err := sk.GetPartitions(sk.topic, sk.addr, sk.cfg)
	if err != nil {
		log.Errorf("get partitions error %v", err)
		return make([]int64, 0), errors.Trace(err)
	}

	return sk.GetAllOffset(partitions, commitTs)
}

func GetSafeTs(ts int64) int64 {
	ts = ts >> shiftBits
	return ts 
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
		var offset int64

		startOffset, err := sk.GetFirstOffset(int32(partition))
		if err != nil {
			log.Errorf("getFirstOffset error %v", err)
			return offsets, errors.Trace(err)
		}

		endOffset, err:= sk.GetLastOffset(int32(partition))
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

func checkArg(topic string, addr []string) error{
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

func (sk *OffsetSeeker)GetPosOffset(partition int32, start int64, end int64, ts int64) (int64, error) {
	for {
		if(start > end){
			break
		}

		mid := (end - start) / 2 + start
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

func (sk *OffsetSeeker)GetOneOffset(partition int32, offset int64, ts int64) (int64,error) {
	bg := new(pb.Binlog)

	pc, err := sk.consumer.ConsumePartition(sk.topic, partition, offset)
	if err != nil {
		log.Errorf("ConsumePartition error %v", err)
		return -1, errors.Trace(err)
	}

	defer pc.AsyncClose()

	for msg := range pc.Messages(){
		err := json.Unmarshal(msg.Value, bg)
		if err != nil {
			log.Errorf("unmarshal binlog error(%v)", err)
			return -1, errors.Trace(err)
		}

		log.Errorf("bg.CommitTs %v %v %v", bg.CommitTs, ts, offset)
		if bg.CommitTs < ts{
			return msg.Offset, nil

		}

		return -1, nil
	}

	return -1, errors.New("cannot find a valid offset")
}

func (sk *OffsetSeeker)GetOffset(partition int32, ts int64) (int64,error){
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


func (sk *OffsetSeeker)GetFirstOffset(partition int32) (int64,error){
	return sk.GetOffset(partition, sarama.OffsetOldest)
}

func (sk *OffsetSeeker)GetLastOffset(partition int32) (int64,error){
	return sk.GetOffset(partition, sarama.OffsetNewest)
}
