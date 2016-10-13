package cistern

import (
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/util/codec"
	"golang.org/x/net/context"
)

const (
	defaultPublishInterval = 1 * time.Second
)

// Publisher periodically updates the lower boundary of deposit window and makes the windows moving forward
// Always guarantees the binlog items out of window are published for drainer.
type Publisher struct {
	interval time.Duration
	period   time.Duration
	window   *DepositWindow
	boltdb   store.Store
}

// NewPublisher return an instance of Publisher
func NewPublisher(cfg *Config, s store.Store, w *DepositWindow) *Publisher {
	return &Publisher{
		interval: defaultPublishInterval,
		period:   time.Duration(cfg.DepositWindowPeriod) * time.Minute,
		window:   w,
		boltdb:   s,
	}
}

// Start run a loop of publishing binlog to drainer
func (p *Publisher) Start(ctx context.Context) {
	round := 1
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.interval):
			log.Debugf("start to run publishing at round[%d]", round)
			start := time.Now()
			if err := p.publish(); err != nil {
				log.Errorf("run publishing error: %v", err)
			}
			elapsed := time.Now().Sub(start)
			log.Debugf("finished publishing at round[%d], elapsed time[%s]", round, elapsed)
			round++
		}
	}
}

func (p *Publisher) publish() error {
	start := p.window.LoadLower()
	startKey := codec.EncodeInt([]byte{}, start)
	end := start

	p.boltdb.RLock()
	defer p.boltdb.RUnlock()

	err := p.boltdb.Scan(BinlogNamespace, startKey, func(key []byte, val []byte) bool {
		_, cts, err := codec.DecodeInt(key)
		if err != nil {
			log.Errorf("run publishing error: %v", err)
			return false
		}

		_, age, err := decodePayload(val)
		if err != nil {
			log.Errorf("run publishing error: %v", err)
			return false
		}

		if age < p.period {
			end = cts
			return false
		}

		return true
	})
	if err != nil {
		return errors.Trace(err)
	}

	if end > start {
		if err := p.window.PersistLower(end); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}
