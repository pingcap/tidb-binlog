package cistern

import (
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/store"
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
	rocksdb  store.Store
}

// NewPublisher return an instance of Publisher
func NewPublisher(cfg *Config, s store.Store, w *DepositWindow) *Publisher {
	return &Publisher{
		interval: defaultPublishInterval,
		period:   time.Duration(cfg.DepositWindowPeriod) * time.Minute,
		window:   w,
		rocksdb:  s,
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
	end := start
	iter, err := p.rocksdb.Scan(start)
	if err != nil {
		return errors.Trace(err)
	}
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		cts, err := iter.CommitTs()
		if err != nil {
			return errors.Trace(err)
		}
		_, age, err := iter.Payload()
		if err != nil {
			return errors.Trace(err)
		}
		if age < p.period {
			end = cts
			break
		}
	}
	if end > start {
		if err := p.window.PersistLower(end); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
