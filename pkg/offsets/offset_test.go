package offsets

import (
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testOffsetSuite{})

type testOffsetSuite struct {
	producer sarama.SyncProducer
}

func (to *testOffsetSuite) TestOffset(c *C) {
	kafkaAddr := "127.0.0.1"
	if os.Getenv("HOSTIP") != "" {
		kafkaAddr = os.Getenv("HOSTIP")
	}
	topic := "test"

	sk, err := NewKafkaSeeker([]string{kafkaAddr + ":9092"}, nil, PositionOperator{})
	c.Assert(err, IsNil)

	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.Return.Successes = true
	to.producer, err = sarama.NewSyncProducer([]string{kafkaAddr + ":9092"}, config)
	c.Assert(err, IsNil)
	defer to.producer.Close()

	var testDatas = []string{"b", "d", "e"}
	var testPoss = map[string]int64{
		"b": 0,
		"d": 0,
		"e": 0,
	}
	for _, m := range testDatas {
		testPoss[m], err = to.procudeMessage([]byte(m), topic)
		c.Assert(err, IsNil)
	}

	var testCases = map[string]int64{
		"a": testPoss["b"],
		"c": testPoss["b"],
		"b": testPoss["b"],
		"h": testPoss["e"],
	}
	for m, res := range testCases {
		offsetFounds, err := sk.Do(topic, m, 0, 0, []int32{0})
		c.Assert(err, IsNil)
		c.Assert(offsetFounds, HasLen, 1)
		c.Assert(offsetFounds[0], Equals, res)
	}
}

func (to *testOffsetSuite) procudeMessage(message []byte, topic string) (int64, error) {
	var (
		offset int64
		err    error
	)
	for i := 0; i < 5; i++ {
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: int32(0),
			Key:       sarama.StringEncoder("key"),
			Value:     sarama.ByteEncoder(message),
		}
		_, offset, err = to.producer.SendMessage(msg)
		if err == nil {
			return offset, errors.Trace(err)
		}

		time.Sleep(time.Second)
	}

	return offset, err
}

type PositionOperator struct{}

// Compare implements Operator.Compare interface
func (p PositionOperator) Compare(exceptedPos interface{}, currentPos interface{}) (int, error) {
	b, ok := currentPos.(string)
	if !ok {
		return 0, errors.Errorf("fail to convert %v type to string", currentPos)
	}

	a, ok := exceptedPos.(string)
	if !ok {
		return 0, errors.Errorf("fail to convert %v type to string", exceptedPos)
	}

	if a > b {
		return 1, nil
	}
	if a == b {
		return 0, nil
	}

	return -1, nil
}

// Decode implements Operator.Decode interface
func (p PositionOperator) Decode(message *sarama.ConsumerMessage) (interface{}, error) {
	return string(message.Value), nil
}
