package offset

import (
	"testing"
	"time"

	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testOffsetSuite{})

type testOffsetSuite struct{}

func (*testOffsetSuite) TestOffset(c *C) {
	var b binlog
	b.topic = "hello1"
	b.addr = []string{"localhost:9092"}
	b.cfg = nil

	producer, err := sarama.NewSyncProducer(b.addr, b.cfg)
	c.Assert(err, IsNil)

	defer producer.Close()

	b.bg.StartTs = int64(1)
	b.bg.CommitTs = int64(1)
	b.bg.PrewriteKey = []byte("key")
	b.bg.PrewriteValue = []byte("value")
	b.bg.DdlQuery = []byte("XXX")
	b.bg.DdlJobId = int64(1)

	res, err := json.Marshal(b.bg)
	msg := &sarama.ProducerMessage{
		Topic:     b.topic,
		Partition: int32(0),
		Key:       sarama.StringEncoder("key"),
		Value:     sarama.ByteEncoder(res),
	}

	_, offset, err := producer.SendMessage(msg)
	c.Assert(err, IsNil)

	getOffset, err := b.findOffsetByTS(time.Now().Unix())
	c.Assert(err, IsNil)

	for i := range getOffset {
		if getOffset[i] > offset {
			log.Errorf("getOffset is large offset")
		}
	}
}
