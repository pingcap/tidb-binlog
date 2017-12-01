package offset

import (
	"testing"
<<<<<<< HEAD
	"time"
=======
//	"time"
>>>>>>> 54da05e580cb1f0b25c1591341c332a3ca5dfc09

	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
<<<<<<< HEAD
=======
	pb "github.com/pingcap/tipb/go-binlog"
>>>>>>> 54da05e580cb1f0b25c1591341c332a3ca5dfc09
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testOffsetSuite{})

type testOffsetSuite struct{}

func (*testOffsetSuite) TestOffset(c *C) {
<<<<<<< HEAD
	var b binlog
	b.topic = "hello1"
	b.addr = []string{"localhost:9092"}
	b.cfg = nil

	producer, err := sarama.NewSyncProducer(b.addr, b.cfg)
=======
	var sk OffsetSeeker
	var bg pb.Binlog
	sk.topic = "hello1"
	sk.addr = []string{"localhost:9092"}
	sk.cfg = nil

	producer, err := sarama.NewSyncProducer(sk.addr, sk.cfg)
>>>>>>> 54da05e580cb1f0b25c1591341c332a3ca5dfc09
	c.Assert(err, IsNil)

	defer producer.Close()

<<<<<<< HEAD
	b.bg.StartTs = int64(1)
	b.bg.CommitTs = int64(1)
	b.bg.PrewriteKey = []byte("key")
	b.bg.PrewriteValue = []byte("value")
	b.bg.DdlQuery = []byte("XXX")
	b.bg.DdlJobId = int64(1)

	res, err := json.Marshal(b.bg)
	msg := &sarama.ProducerMessage{
		Topic:     b.topic,
=======
	bg.PrewriteKey = []byte("key")
	bg.PrewriteValue = []byte("value")

	res, err := json.Marshal(bg)
	msg := &sarama.ProducerMessage{
		Topic:     sk.topic,
>>>>>>> 54da05e580cb1f0b25c1591341c332a3ca5dfc09
		Partition: int32(0),
		Key:       sarama.StringEncoder("key"),
		Value:     sarama.ByteEncoder(res),
	}

	_, offset, err := producer.SendMessage(msg)
	c.Assert(err, IsNil)

<<<<<<< HEAD
	getOffset, err := b.findOffsetByTS(time.Now().Unix())
	c.Assert(err, IsNil)

=======
	getOffset, err := sk.FindOffsetByTS(int64(1 << 18))
	c.Assert(err, IsNil)
	
	log.Errorf("getOffset is %v", getOffset)
>>>>>>> 54da05e580cb1f0b25c1591341c332a3ca5dfc09
	for i := range getOffset {
		if getOffset[i] > offset {
			log.Errorf("getOffset is large offset")
		}
	}
}
