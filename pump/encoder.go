package pump

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

var magic uint32 = 471532804

type CompressionCodec int8

const (
	CompressionNone = iota
	CompressionGZIP
	CompressionSnappy // not supported yet
	CompressionLZ4    // ditto
)

// ToCompressionCodec converts v to CompressionCodec.
func ToCompressionCodec(v string) CompressionCodec {
	v = strings.ToLower(v)
	switch v {
	case "":
		return CompressionNone
	case "gzip":
		return CompressionGZIP
	case "lz4":
		return CompressionLZ4
	case "snappy":
		return CompressionSnappy
	default:
		log.Warnf("unknown codec %v, no compression.", v)
		return CompressionNone
	}
}

//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  | magic word (4 byte)| Size (8 byte, len(payload)) |    payload    |  crc  |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

// Encoder contains Encode method which encodes payload and write it, and returns offset.
type Encoder interface {
	Encode(payload []byte) (int64, error)
}

type encoder struct {
	bw    io.Writer
	codec CompressionCodec
}

func newEncoder(w io.Writer, codec CompressionCodec) Encoder {
	return &encoder{
		bw:    w,
		codec: codec,
	}
}

func (e *encoder) Encode(payload []byte) (int64, error) {
	data := encode(payload)

	switch e.codec {
	case CompressionNone:
		// nothing to do
	case CompressionGZIP:
		var buf bytes.Buffer
		writer := gzip.NewWriter(&buf)
		if _, err := writer.Write(data); err != nil {
			return 0, err
		}
		if err := writer.Close(); err != nil {
			return 0, err
		}
		data = buf.Bytes()
	case CompressionLZ4:
		panic("not implemented yet")
	case CompressionSnappy:
		panic("not implemented yet")
	}
	_, err := e.bw.Write(data)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if file, ok := e.bw.(*os.File); ok {
		curOffset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return curOffset, nil
	}

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
