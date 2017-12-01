package offset

import (
	"testing"
//	"time"

	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testOffsetSuite{})

type testOffsetSuite struct{}

func (*testOffsetSuite) TestOffset(c *C) {
	var sk OffsetSeeker
	var bg pb.Binlog
	sk.topic = "hello"
	sk.addr = []string{"localhost:9092"}
	sk.cfg = nil

	producer, err := sarama.NewSyncProducer(sk.addr, sk.cfg)
	c.Assert(err, IsNil)

	defer producer.Close()

	bg.PrewriteKey = []byte("key")
	bg.PrewriteValue = []byte("value")

	res, err := json.Marshal(bg)
	msg := &sarama.ProducerMessage{
		Topic:     sk.topic,
		Partition: int32(0),
		Key:       sarama.StringEncoder("key"),
		Value:     sarama.ByteEncoder(res),
	}

	_, offset, err := producer.SendMessage(msg)
	c.Assert(err, IsNil)

	getOffset, err := sk.FindOffsetByTS(int64(1 << 18))
	c.Assert(err, IsNil)
	
	log.Errorf("getOffset is %v", getOffset)
	for i := range getOffset {
		if getOffset[i] > offset {
			log.Errorf("getOffset is large offset")
		}
	}
}
