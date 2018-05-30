package drainer

import (
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/assemble"
	"github.com/pingcap/tidb-binlog/pkg/offsets"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
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
func (s *seekOperator) Decode(ctx context.Context, messages <-chan *sarama.ConsumerMessage) (interface{}, error) {
	errCounter := prometheus.NewCounter(prometheus.CounterOpts{})
	asm := assemble.NewAssembler(errCounter)
	defer asm.Close()

	var binlog *assemble.AssembledBinlog
	for {
		select {
		case <-ctx.Done():
			log.Warningf("offset seeker was canceled: %v", ctx.Err())
			return nil, errors.New("offset seeker was canceled")
		case msg := <-messages:
			asm.Append(msg)
		case binlog = <-asm.Messages():
		}
		if binlog == nil {
			continue
		}
		break
	}

	bg := new(pb.Binlog)
	err := bg.Unmarshal(binlog.Entity.Payload)
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
