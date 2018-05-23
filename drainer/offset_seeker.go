package drainer

import (
	"bytes"
	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/offsets"
	"github.com/pingcap/tidb-binlog/pkg/slicer"
	pb "github.com/pingcap/tipb/go-binlog"
)

type seekOperator struct{}

// Compare implements Operator.Compare interface
func (s *seekOperator) Compare(exceptedPos interface{}, currentPos interface{}) (int, error) {
	b, ok := currentPos.(int64)
	if !ok {
		log.Errorf("convert %d to Int64 error", b)
		return 0, errors.New("connot conver to Int64")
	}

	a, ok := exceptedPos.(int64)
	if !ok {
		log.Errorf("convert %d to Int64 error", a)
		return 0, errors.New("connot conver to Int64")
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
func (s *seekOperator) Decode(messages <-chan *sarama.ConsumerMessage) (interface{}, error) {
	bg := new(pb.Binlog)

	var payload []byte
	msg := <-messages
	if len(msg.Headers) == 0 {
		// unsplited message
		payload = msg.Value
	} else {
		var messageID []byte
		for {
			// find the first slice of a message
			noByte := slicer.GetValueFromComsumerMessageHeader(slicer.No, msg)
			no := binary.LittleEndian.Uint32(noByte)
			if no == 0 {
				payload = make([]byte, 0, 1024*1024)
				payload = append(payload, msg.Value...)
				messageID = slicer.GetValueFromComsumerMessageHeader(slicer.MessageID, msg)
				break
			}
			msg = <-messages
		}
		for msg := range messages {
			messageIDNew := slicer.GetValueFromComsumerMessageHeader(slicer.MessageID, msg)
			if !bytes.Equal(messageID, messageIDNew) {
				return nil, errors.Errorf("decode messageID %v mismatch %v", messageID, messageIDNew)
			}
			payload = append(payload, msg.Value...)
			if slicer.GetValueFromComsumerMessageHeader(slicer.Checksum, msg) != nil {
				break // assembled a complete message
			}
		}
	}
	err := bg.Unmarshal(payload)
	if err != nil {
		log.Errorf("json umarshal error %v", err)
		return nil, errors.Trace(err)
	}

	ts := bg.GetCommitTs()
	if ts == 0 {
		ts = bg.GetStartTs()
	}

	return ts, nil
}

func createOffsetSeeker(addrs []string, kafkaVersion string) (offsets.Seeker, error) {
	cfg := sarama.NewConfig()
	version, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cfg.Version = version
	log.Infof("kafka consumer version %v", version)

	seeker, err := offsets.NewKafkaSeeker(addrs, cfg, &seekOperator{})
	return seeker, errors.Trace(err)
}
