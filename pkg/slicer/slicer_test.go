package slicer

import (
	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"math"
	"os"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSlicerSuite{})

type testSlicerSuite struct {
	producer sarama.SyncProducer
}

func (ts *testSlicerSuite) TestTracker(c *C) {
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
	ts.deleteTopic(kafkaAddr, config, topic, c)
	// tear down or clear up
	defer ts.deleteTopic(kafkaAddr, config, topic, c)

	kt, err := NewKafkaTracker([]string{kafkaAddr}, config)
	c.Assert(err, IsNil)
	defer kt.Close()

	ts.producer, err = sarama.NewSyncProducer([]string{kafkaAddr}, config)
	c.Assert(err, IsNil)
	defer ts.producer.Close()

	var offsetExpected int64

	// unsplit binlog
	messageIDToSend := []byte("message1")
	messageToSend := []byte("a test message 1 for slicer tracker")
	slicesToSend := ts.testSlicesSplit(topic, messageIDToSend, messageToSend, 1, c)
	offset, offsetExpected := ts.testSlicesSend(slicesToSend, offsetExpected, c)
	messageIDReceive, messageReceive := ts.testSlicesTracker(kt, topic, offset, messageToSend, c)
	c.Assert(messageIDToSend, DeepEquals, messageIDReceive)
	c.Assert(messageReceive, DeepEquals, messageToSend)

	// split binlog
	messageIDToSend = []byte("message2")
	messageToSend = []byte("a test message 2 for slicer tracker")
	slicesToSend = ts.testSlicesSplit(topic, messageIDToSend, messageToSend, 4, c)
	offset, offsetExpected = ts.testSlicesSend(slicesToSend, offsetExpected, c)
	messageIDReceive, messageReceive = ts.testSlicesTracker(kt, topic, offset, messageToSend, c)
	c.Assert(messageIDToSend, DeepEquals, messageIDReceive)
	c.Assert(messageReceive, DeepEquals, messageToSend)

	messageIDReceive, messageReceive = ts.testSlicesTracker(kt, topic, offset-1, messageToSend, c)
	c.Assert(messageIDToSend, Not(DeepEquals), messageIDReceive)
	c.Assert(messageReceive, Not(DeepEquals), messageToSend)

	// duplicate slices
	messageIDToSend = []byte("message3")
	messageToSend = []byte("a test message 3 for slicer tracker")
	slicesToSend = ts.testSlicesSplit(topic, messageIDToSend, messageToSend, 3, c)
	slicesDuplicated := make([]interface{}, len(slicesToSend)+2)
	slicesDuplicated[0] = slicesToSend[0]
	slicesDuplicated[1] = slicesToSend[0]
	slicesDuplicated[2] = slicesToSend[1]
	slicesDuplicated[3] = slicesToSend[1]
	slicesDuplicated[4] = slicesToSend[2]
	offset, offsetExpected = ts.testSlicesSend(slicesDuplicated, offsetExpected, c)
	messageIDReceive, messageReceive = ts.testSlicesTracker(kt, topic, offset, messageToSend, c)
	c.Assert(messageIDToSend, DeepEquals, messageIDReceive)
	c.Assert(messageReceive, DeepEquals, messageToSend)

	messageIDReceive, messageReceive = ts.testSlicesTracker(kt, topic, offset-3, messageToSend, c)
	c.Assert(messageIDToSend, DeepEquals, messageIDReceive)
	c.Assert(messageReceive, DeepEquals, messageToSend)

	// out-of-order slices
	messageIDToSend = []byte("message4")
	messageToSend = []byte("a test message 4 for slicer tracker")
	slicesToSend = ts.testSlicesSplit(topic, messageIDToSend, messageToSend, 3, c)
	slicesOutOfOrder := make([]interface{}, len(slicesToSend))
	slicesOutOfOrder[1] = slicesToSend[1]
	slicesOutOfOrder[2], slicesOutOfOrder[0] = slicesToSend[0], slicesToSend[2]
	offset, offsetExpected = ts.testSlicesSend(slicesOutOfOrder, offsetExpected, c)
	messageIDReceive, messageReceive = ts.testSlicesTracker(kt, topic, offset, messageToSend, c)
	c.Assert(messageIDToSend, DeepEquals, messageIDReceive)
	c.Assert(messageReceive, DeepEquals, messageToSend)

	messageIDReceive, messageReceive = ts.testSlicesTracker(kt, topic, offset-3, messageToSend, c)
	c.Assert(messageIDToSend, DeepEquals, messageIDReceive)
	c.Assert(messageReceive, DeepEquals, messageToSend)
}

func (ts *testSlicerSuite) testSlicesSplit(topic string, messageID []byte, message []byte, preferSliceCount int, c *C) []interface{} {
	slicesToSend, err := ts.splitMessageToSlices(topic, messageID, message, preferSliceCount)
	c.Assert(err, IsNil)
	return slicesToSend
}

func (ts *testSlicerSuite) testSlicesSend(slicesToSend []interface{}, offsetExpected int64, c *C) (int64, int64) {
	offset, err := ts.produceMessageSlices(slicesToSend)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, offsetExpected)
	return offset, offsetExpected + int64(len(slicesToSend))
}

func (ts *testSlicerSuite) testSlicesTracker(kt Tracker, topic string, offset int64, messageToSend []byte, c *C) ([]byte, []byte) {
	slicesReceive, err := kt.Slices(topic, 0, offset)
	c.Assert(err, IsNil)
	messageID, message, err := ts.getMessageFromSlices(slicesReceive)
	c.Assert(err, IsNil)
	return messageID, message
}

func (ts *testSlicerSuite) deleteTopic(kafkaAddr string, config *sarama.Config, topic string, c *C) {
	// delete topic to clear produced messages
	broker := sarama.NewBroker(kafkaAddr)
	err := broker.Open(config)
	c.Assert(err, IsNil)
	_, err = broker.Connected()
	c.Assert(err, IsNil)
	defer broker.Close()
	broker.DeleteTopics(&sarama.DeleteTopicsRequest{Topics: []string{topic}, Timeout: 30 * time.Second})
}

func (ts *testSlicerSuite) produceMessageSlices(slices []interface{}) (int64, error) {
	var (
		offset int64
		err    error
		j      int
		slice  interface{}
	)
	for i := 0; i < 5; i++ {
		for j, slice = range slices {
			_, offsetSlice, err := ts.producer.SendMessage(slice.(*sarama.ProducerMessage))
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

func (ts *testSlicerSuite) splitMessageToSlices(topic string, messageID []byte, message []byte, preferSliceCount int) ([]interface{}, error) {
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
					Key:   MessageID,
					Value: messageID,
				},
				{
					Key:   No,
					Value: no,
				}, {
					Key:   Total,
					Value: total,
				},
			},
		}
		if i == sliceCount-1 {
			// last slice, append checksum
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   Checksum,
				Value: checksum,
			})
		}
		slices[i] = msg
	}
	return slices, nil
}

func (ts *testSlicerSuite) getMessageFromSlices(slices []interface{}) ([]byte, []byte, error) {
	var (
		messageID []byte
		payload   = make([]byte, 0, 1024*1024)
	)
	for _, slice := range slices {
		msg := slice.(*sarama.ConsumerMessage)
		messageID = GetValueFromComsumerMessageHeader(MessageID, msg)
		payload = append(payload, msg.Value...)
	}
	return messageID, payload, nil
}
