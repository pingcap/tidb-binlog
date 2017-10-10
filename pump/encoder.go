package pump

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"github.com/ngaut/log"
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
)

var magic uint32 = 471532804

//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  | magic word (4 byte)| Size (8 byte, len(payload)) |    payload    |  crc  |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type encoder struct {
	bw io.Writer
}

func newEncoder(w io.Writer) *encoder {
	return &encoder{
		bw: w,
	}
}

func (e *encoder) encode(payload []byte) error {
	data := encode(payload)
	_, err := e.bw.Write(data)
	return errors.Trace(err)
}

type kafkaEncoder struct {
	topic     string
	partition int32
	producer  sarama.SyncProducer
}

func newKafkaEncoder(producer sarama.SyncProducer, topic string, partition int32) *kafkaEncoder {
	return &kafkaEncoder{
		producer:  producer,
		topic:     topic,
		partition: partition,
	}
}

func (k *kafkaEncoder) encode(payload []byte) (int64, error) {
	msg := &sarama.ProducerMessage{Topic: k.topic, Partition: k.partition, Value: sarama.ByteEncoder(payload)}
	partition, offset, err := k.producer.SendMessage(msg)
	if err != nil {
		log.Infof("send message error: %s, msg: %s, partition: %s, offset: %s, topic: %s", err, msg, partition, offset, k.topic)
		return 0, errors.Trace(err)
	}

	if partition != k.partition {
		return 0, errors.Errorf("produce message to wrong partition %d, not specified partition %d", partition, k.partition)
	}

	return offset, nil
}

func encode(payload []byte) []byte {
	crc := crc32.Checksum(payload, crcTable)

	// length count payload
	length := len(payload)

	// size is length of magic + size + crc + payload
	size := length + 16
	data := make([]byte, size)

	binary.LittleEndian.PutUint32(data[:4], magic)
	binary.LittleEndian.PutUint64(data[4:12], uint64(length))
	copy(data[12:size-4], payload)
	binary.LittleEndian.PutUint32(data[size-4:], crc)
	return data
}
