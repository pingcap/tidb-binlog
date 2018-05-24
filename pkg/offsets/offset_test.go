package offsets

import (
	"os"
	"testing"
	"time"

	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/slicer"
	"math"
	"math/rand"
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
	kafkaAddr = kafkaAddr + ":9092"
	topic := "test"

	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.Return.Successes = true

	// clear previous tests produced
	to.deleteTopic(kafkaAddr, config, topic, c)
	// tear down or clear up
	defer to.deleteTopic(kafkaAddr, config, topic, c)

	sk, err := NewKafkaSeeker([]string{kafkaAddr}, config, PositionOperator{})
	c.Assert(err, IsNil)
	defer sk.Close()

	to.producer, err = sarama.NewSyncProducer([]string{kafkaAddr}, config)
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

	// offset seek for slice messages, out-of-order
	message := []byte("aaaaaaaaaaaaaaaaaaaa")
	slices, err := to.splitMessageToSlices(topic, []byte("messageID1"), message, 4)
	rand.Shuffle(len(slices), func(i, j int) {
		slices[i], slices[j] = slices[j], slices[i]
	})
	offset, err := to.produceMessageSlices(slices)
	c.Assert(err, IsNil)
	offsetFounds, err := sk.Do(topic, string(message), 0, 0, []int32{0})
	c.Assert(err, IsNil)
	c.Assert(offsetFounds, HasLen, 1)
	c.Assert(offsetFounds[0], Equals, offset)
}

func (to *testOffsetSuite) deleteTopic(kafkaAddr string, config *sarama.Config, topic string, c *C) {
	// delete topic to clear produced messages
	broker := sarama.NewBroker(kafkaAddr)
	err := broker.Open(config)
	c.Assert(err, IsNil)
	_, err = broker.Connected()
	c.Assert(err, IsNil)
	defer broker.Close()
	broker.DeleteTopics(&sarama.DeleteTopicsRequest{Topics: []string{topic}, Timeout: 30 * time.Second})
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

func (to *testOffsetSuite) produceMessageSlices(slices []interface{}) (int64, error) {
	var (
		offset int64
		err    error
		j      int
		slice  interface{}
	)
	for i := 0; i < 5; i++ {
		for j, slice = range slices {
			_, offsetSlice, err := to.producer.SendMessage(slice.(*sarama.ProducerMessage))
			if err != nil {
				if j == 0 {
					time.Sleep(time.Second)
					break // the first slice send fail, outer for loop try again
				}
				return offset, errors.Trace(err)
			}
			if j == 0 {
				offset = offsetSlice // saves for return
			}
		}
		if j == len(slices)-1 {
			break // all slices sent
		}
	}
	return offset, err
}

func (to *testOffsetSuite) splitMessageToSlices(topic string, messageID []byte, message []byte, preferSliceCount int) ([]interface{}, error) {
	sliceLen := int(math.Ceil(float64(len(message)) / float64(preferSliceCount)))
	if sliceLen <= 0 {
		sliceLen = 1
	}
	sliceCount := int(math.Ceil(float64(len(message)) / float64(sliceLen)))
	slices := make([]interface{}, sliceCount)

	var (
		total    = make([]byte, 4)
		checksum = []byte("hash")
	)
	binary.LittleEndian.PutUint32(total, uint32(sliceCount))

	for i := 0; i < sliceCount; i++ {
		startIdx, endIdx := i*sliceLen, (i+1)*sliceLen
		if i == sliceCount-1 {
			endIdx = len(message)
		}
		payload := message[startIdx:endIdx]
		no := make([]byte, 4)
		binary.LittleEndian.PutUint32(no, uint32(i))
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: int32(0),
			Key:       sarama.StringEncoder("key"),
			Value:     sarama.ByteEncoder(payload),
			Headers: []sarama.RecordHeader{
				{
					Key:   slicer.MessageID,
					Value: messageID,
				},
				{
					Key:   slicer.No,
					Value: no,
				}, {
					Key:   slicer.Total,
					Value: total,
				},
			},
		}
		if i == sliceCount-1 {
			// last slice, append checksum
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   slicer.Checksum,
				Value: checksum,
			})
		}
		slices[i] = msg
	}
	return slices, nil
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

	if a > b || len(a) > len(b) { // maybe a slice message
		return 1, nil
	}
	if a == b {
		return 0, nil
	}

	return -1, nil
}

// Decode implements Operator.Decode interface
func (p PositionOperator) Decode(slices []interface{}) (interface{}, error) {
	var payload []byte
	if len(slices) == 1 {
		msg := slices[0].(*sarama.ConsumerMessage)
		payload = msg.Value
	} else {
		payload = make([]byte, 0, 1024*1024)
		for _, slice := range slices {
			msg := slice.(*sarama.ConsumerMessage)
			payload = append(payload, msg.Value...)
		}
	}
	return string(payload), nil
}
