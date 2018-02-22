package pump

import (
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	pkgfile "github.com/pingcap/tidb-binlog/pkg/file"
)

var magic uint32 = 471532804

//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  | magic word (4 byte)| Size (8 byte, len(payload)) |    payload    |  crc  |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

// Encoder contains Encode method which encodes payload and write it, and returns offset.
type Encoder interface {
	Encode(payload []byte) (int64, error)
}

type encoder struct {
	bw    io.Writer
	codec compress.CompressionCodec
}

func newEncoder(w io.Writer, codec compress.CompressionCodec) Encoder {
	return &encoder{
		bw:    w,
		codec: codec,
	}
}

func (e *encoder) Encode(payload []byte) (int64, error) {
	data := encode(payload)

	data, err := compress.Compress(data, e.codec)
	if err != nil {
		return 0, errors.Trace(err)
	}
	_, err = e.bw.Write(data)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if file, ok := e.bw.(*pkgfile.LockedFile); ok {
		curOffset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return curOffset, nil
	}
	log.Warn("bw is not *file.Lockedfile, unexpected!")
	return 0, errors.Trace(err)
}

type kafkaEncoder struct {
	topic     string
	partition int32
	producer  sarama.SyncProducer
}

func newKafkaEncoder(producer sarama.SyncProducer, topic string, partition int32) Encoder {
	return &kafkaEncoder{
		producer:  producer,
		topic:     topic,
		partition: partition,
	}
}

func (k *kafkaEncoder) Encode(payload []byte) (int64, error) {
	msg := &sarama.ProducerMessage{Topic: k.topic, Partition: k.partition, Value: sarama.ByteEncoder(payload)}
	partition, offset, err := k.producer.SendMessage(msg)
	if err != nil {
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
