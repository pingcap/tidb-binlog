package pump

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	binlog "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
)

const switchDetectInterval = 10 * time.Second

// Proxy is a proxy binlogger
type Proxy struct {
	sync.RWMutex

	master Binlogger
	slave  Binlogger

	enableSwitch bool

	ctx    context.Context
	cancel context.CancelFunc
}

func newProxy(master, slave Binlogger, enableProxySwitch bool) Binlogger {
	p := &Proxy{
		master: master,
		slave:  slave,

		enableSwitch: enableProxySwitch,
	}

	p.ctx, p.cancel = context.WithCancel(context.Background())
	return p
}

// ReadFrom implements ReadFrom WriteTail interface
func (p *Proxy) ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error) {
	return p.master.ReadFrom(from, nums)
}

// WriteTail implements Binlogger WriteTail interface
func (p *Proxy) WriteTail(payload []byte) error {
	p.Lock()
	defer p.Unlock()

	var err error
	if p.master.IsAvailable() {
		err = p.master.WriteTail(payload)
	} else {
		err = errors.New("binlogger is not avaliable")
	}

	if !p.enableSwitch {
		return errors.Trace(err)
	}

	err = p.slave.WriteTail(payload)
	return errors.Trace(err)
}

// IsAvailable implements Binlogger IsAvailable interface
func (p *Proxy) IsAvailable() bool {
	return true
}

// MarkAvailable implements binlogger MarkAvailable interface
func (p *Proxy) MarkAvailable() {}

// Close closes the binlogger
func (p *Proxy) Close() error {
	var err error
	if p.master != nil {
		err = p.master.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}

	if p.slave != nil {
		err = p.slave.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}

	p.cancel()
	return nil
}

// GC recycles the old binlog file
func (p *Proxy) GC(days time.Duration) {
	p.master.GC(days)
}

func (p *Proxy) switchMS() {
	ticker := time.NewTicker(switchDetectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			log.Info("context cancel in proxy")
			return
		case <-ticker.C:
			if !p.master.IsAvailable() {
				if err := p.migrateBinlog(); err != nil {
					log.Errorf("migrate binlogs error %v", err)
				}
			}
		}
	}
}

func (p *Proxy) migrateBinlog() error {
	for {
		p.RLock()
		entities, err := p.slave.ReadFrom(binlog.Pos{}, 0)
		if err != nil {
			p.RUnlock()
			return errors.Trace(err)
		}
		if len(entities) == 0 {
			p.master.MarkAvailable()
		}
		p.RUnlock()

		for _, entity := range entities {
			err = p.master.WriteTail(entity.Payload)
			if err != nil {
				return errors.Trace(err)
			}
		}

		_, err = p.slave.ReadFrom(binlog.Pos{}, 1)
		if err != nil {
			return errors.Trace(err)
		}
	}
}
