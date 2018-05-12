package pump

import (
	"encoding/binary"
	"fmt"

	"github.com/Shopify/sarama"
	binlog "github.com/pingcap/tipb/go-binlog"
)

// kafkaSlicer spit payload into multiple messages
type kafkaSlicer struct {
	topic     string
	partition int32
}

func newKafkaSlicer(topic string, partition int32) *kafkaSlicer {
	return &kafkaSlicer{
		topic:     topic,
		partition: partition,
	}
}

func (s *kafkaSlicer) Generate(entity *binlog.Entity) ([]*sarama.ProducerMessage, error) {
	if !GlobalConfig.enableBinlogSlice {
		return []*sarama.ProducerMessage{
			{
				Topic:     s.topic,
				Partition: s.partition,
				Value:     sarama.ByteEncoder(entity.Payload),
			},
		}, nil
	}

	var (
		// hard code, lenght of checksum code is 4
		total     = (len(entity.Payload) + GlobalConfig.slicesSize - 1) / GlobalConfig.slicesSize
		messages  = make([]*sarama.ProducerMessage, 0, total)
		left      = 0
		right     = 0
		totalByte = make([]byte, 4)
		messageID = []byte(fmt.Sprintf("%d-%d", entity.Pos.Suffix, entity.Pos.Offset))
	)

	binary.LittleEndian.PutUint32(totalByte, uint32(total))
	for i := 0; i < total-1; i++ {
		no := make([]byte, 4)

		binary.LittleEndian.PutUint32(no, uint32(i))
		right = left + GlobalConfig.slicesSize
		messages = append(messages, &sarama.ProducerMessage{
			Topic:     s.topic,
			Partition: s.partition,
			Value:     sarama.ByteEncoder(entity.Payload[left:right]),
			Headers: []sarama.RecordHeader{
				{
					Key:   []byte("messageID"),
					Value: []byte(messageID),
				}, {
					Key:   []byte("No"),
					Value: no,
				}, {
					Key:   []byte("checksum"),
					Value: entity.Checksum,
				}, {
					Key:   []byte("total"),
					Value: totalByte,
				},
			},
		})
	}

	return messages, nil
}
