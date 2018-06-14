package pump

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	pkgfile "github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tipb/go-binlog"
)

var magic uint32 = 471532804

//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  | magic word (4 byte)| Size (8 byte, len(payload)) |    payload    |  crc  |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

// Encoder is an interface wraps basic Encode method which encodes payload and write it, and returns offset.
type Encoder interface {
	Encode(entity *binlog.Entity) (int64, error)
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

func (e *encoder) Encode(entity *binlog.Entity) (int64, error) {
	data := encode(entity.Payload)

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
	slicer    *KafkaSlicer
}

func newKafkaEncoder(producer sarama.SyncProducer, topic string, partition int32) Encoder {
	return &kafkaEncoder{
		producer:  producer,
		topic:     topic,
		partition: partition,
		slicer:    NewKafkaSlicer(topic, partition),
	}
}

func (k *kafkaEncoder) Encode(entity *binlog.Entity) (int64, error) {
	var (
		messages    []*sarama.ProducerMessage
		errMessages []*sarama.ProducerMessage
		err         error
		retry       int
		offset      int64 = math.MaxInt64
	)

	messages, err = k.slicer.Generate(entity)
	if err != nil {
		return 0, errors.Trace(err)
	}

	err = k.producer.SendMessages(messages)
	for err != nil && retry < GlobalConfig.sendKafKaRetryNum {
		producerErrors, ok := err.(sarama.ProducerErrors)
		if !ok {
			return 0, errors.Trace(err)
		}
		retry++

		log.Errorf("[encoder] produce meesage error %v", producerErrors.Error())

		errMessages = errMessages[:0]
		for _, errMessage := range producerErrors {
			log.Errorf("[encode] produce message %s, now retry No. %d", errMessage.Error(), retry)
			errMessages = append(errMessages, errMessage.Msg)
		}
		err = k.producer.SendMessages(errMessages)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}

	for _, message := range messages {
		if offset > message.Offset {
			offset = message.Offset
		}

		if message.Partition != k.partition {
			return 0, errors.Errorf("produce message to wrong partition %d, not specified partition %d", message.Partition, k.partition)
		}
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
