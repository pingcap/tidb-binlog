package slicer

import (
	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
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

	message := []byte("a test message for slicer tracker")
	offset, err := ts.produceMessageSlices(message, topic)
	c.Assert(err, IsNil)

	slices, err := kt.Slices(topic, 0, offset)
	c.Assert(err, IsNil)
	msg, err := ts.getMessageFromSlices(slices)
	c.Assert(err, IsNil)
	c.Assert(msg, DeepEquals, message)
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

func (ts *testSlicerSuite) produceMessageSlices(message []byte, topic string) (int64, error) {
	sliceCount := 4
	sliceLen := int(math.Ceil(float64(len(message)) / float64(sliceCount)))
	if sliceLen <= 0 {
		sliceLen = 1
	}
	sliceCount = int(math.Ceil(float64(len(message)) / float64(sliceLen)))
	var (
		offset    int64
		err       error
		messageID = []byte("MessageID")
		total     = make([]byte, 4)
		checksum  = []byte("hash")
	)
	binary.LittleEndian.PutUint32(total, uint32(sliceCount))
	slices := make([][]byte, 0, sliceCount)
	for i := 0; i < 5; i++ {
		for j := 0; j < sliceCount; j++ {
			startIdx, endIdx := j*sliceLen, (j+1)*sliceLen
			if j == sliceCount-1 {
				endIdx = len(message)
			}
			slice := message[startIdx:endIdx]
			no := make([]byte, 4)
			binary.LittleEndian.PutUint32(no, uint32(j))
			msg := &sarama.ProducerMessage{
				Topic:     topic,
				Partition: int32(0),
				Key:       sarama.StringEncoder("key"),
				Value:     sarama.ByteEncoder(slice),
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
			log.Info("no", no)
			if j == sliceCount-1 {
				// last slice, append checksum
				msg.Headers = append(msg.Headers, sarama.RecordHeader{
					Key:   Checksum,
					Value: checksum,
				})
			}
			_, offsetSlice, err := ts.producer.SendMessage(msg)
			if err != nil {
				if j == 0 {
					time.Sleep(time.Second)
					break // the first slice send fail, outer for loop try again
				}
				return offset, errors.Trace(err)
			}
			slices = append(slices, slice)
			if j == 0 {
				offset = offsetSlice // use the first slice's offset
			}
		}
		break
	}
	return offset, err
}

func (ts *testSlicerSuite) getMessageFromSlices(slices []interface{}) ([]byte, error) {
	payload := make([]byte, 0, 1024*1024)
	for _, slice := range slices {
		payload = append(payload, slice.(*sarama.ConsumerMessage).Value...)
	}
	return payload, nil
}
