package offsets

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testOffsetSuite{})

type testOffsetSuite struct{}

const shiftBits = 18
const subTime = 20 * 60 * 1000

func (*testOffsetSuite) TestOffset(c *C) {
	addr := os.Getenv("HOSTIP")
	topic := "wangkai"
	
	log.Infof("kafka address is %v", addr)
	
	sk, err := NewKafkaSeeker(topic, []string{addr}, nil, Int64(0))
	c.Assert(err, IsNil)

	var bg pb.Binlog

	producer, err := sarama.NewSyncProducer([]string{addr}, nil)
	c.Assert(err, IsNil)

	defer producer.Close()

	bg.PrewriteKey = []byte("key")
	bg.PrewriteValue = []byte("value")

	res, err := json.Marshal(bg)
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: int32(0),
		Key:       sarama.StringEncoder("key"),
		Value:     sarama.ByteEncoder(res),
	}

	_, offset, err := producer.SendMessage(msg)
	c.Assert(err, IsNil)

	getOffset, err := sk.Do(topic, GetSafeTS(int64(1)), 0, 0)
	c.Assert(err, IsNil)

	log.Infof("getOffset is %v", getOffset)
	for i := range getOffset {
		if getOffset[i] > offset {
			log.Errorf("getOffset is large offset")
		}
	}
}

type Int64 int64

// int64 implements Operator.Compare interface
func (Int64) Compare(exceptedPos interface{}, currentPos interface{}) (int, error) {
	b, ok := currentPos.(int64)
	if !ok {
		log.Errorf("convert to Int64 error", b)
		return 0, errors.New("connot conver to Int64")
	}

	a, ok := exceptedPos.(int64)
	if !ok {
		log.Errorf("convert to Int64 error", a)
		return 0, errors.New("connot conver to Int64")
	}

	if a > b {
		return 1, nil
	}
	if a == b {
		return 0, nil
	}

	return -1, nil
}

// int64 implements Operator.Decode interface
func (Int64) Decode(message *sarama.ConsumerMessage) (interface{}, error) {
	bg := new(pb.Binlog)

	err := json.Unmarshal(message.Value, bg)
	if err != nil {
		log.Errorf("json umarshal error %v", err)
		return nil, errors.Trace(err)
	}

	return bg, nil
}

func GetSafeTS(ts int64) int64 {
	ts = ts >> shiftBits
	ts -= subTime
	if ts < int64(0) {
		ts = int64(0)
	}

	return ts
}
