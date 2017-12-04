package drainer

import (
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/offsets"
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
func (s *seekOperator) Decode(message *sarama.ConsumerMessage) (interface{}, error) {
	bg := new(pb.Binlog)

	err := json.Unmarshal(message.Value, bg)
	if err != nil {
		log.Errorf("json umarshal error %v", err)
		return nil, errors.Trace(err)
	}

	return bg.CommitTs, nil
}

func createOffsetSeeker(addrs []string) (offsets.Seeker, error) {
	seeker, err := offsets.NewKafkaSeeker(addrs, sarama.NewConfig(), &seekOperator{})
	return seeker, errors.Trace(err)
}
