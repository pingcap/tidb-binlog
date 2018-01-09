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
// sync binlog from master and replicate
// if master has error,  switch master and slave
type Proxy struct {
	sync.RWMutex
	wg sync.WaitGroup

	master    Binlogger
	replicate Binlogger
	cp        *checkPoint

	enableSwitch bool

	ctx    context.Context
	cancel context.CancelFunc
}

func newProxy(master, replicate Binlogger, cp *checkPoint, enableProxySwitch bool) Binlogger {
	p := &Proxy{
		master:    master,
		replicate: replicate,
		cp:        cp,

		enableSwitch: enableProxySwitch,
	}

	go p.sync()

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

	err := p.master.WriteTail(payload)
	if err != nil {
		log.Errorf("write binlog error %v", err)
	}

	if p.enableSwitch {
		return nil
	}

	return errors.Trace(err)
}

// Close closes the binlogger
func (p *Proxy) Close() error {
	p.Lock()
	defer p.Unlock()

	var err error

	for {
		pos := p.cp.pos()
		entities, err := p.master.ReadFrom(pos, 1)
		if err == nil {
			if len(entities) == 0 {
				break
			}

			// compute next binlog offset
			entities[0].Pos.Offset += int64(len(entities[0].Payload) + 16)
			if ComparePos(entities[0].Pos, latestFilePos) == 0 {
				break
			}
		}
		if err != nil {
			log.Errorf("read binlogs from master in close error %v", err)
		}

		time.Sleep(time.Second)
	}

	if p.master != nil {
		err = p.master.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}

	p.cancel()
	p.wg.Wait()

	return nil
}

// GC recycles the old binlog file
func (p *Proxy) GC(days time.Duration, pos binlog.Pos) {
	p.master.GC(days, p.cp.pos())
}

func (p *Proxy) sync() error {
	p.wg.Add(1)
	defer p.wg.Done()

	pos := p.cp.pos()
	for {
		select {
		case <-p.ctx.Done():
			log.Info("context cancel - sycner exists")
			return nil
		default:
		}

		entities, err := p.master.ReadFrom(pos, 1000)
		if err != nil {
			log.Errorf("read binlogs from master error %v", err)
		}

		for _, ent := range entities {
			err = p.replicate.WriteTail(ent.Payload)
			if err != nil {
				log.Errorf("write binlog to replicate error %v", err)

				select {
				case <-p.ctx.Done():
					log.Info("context cancel - sycner exists")
					return nil
				case <-time.After(10 * time.Second):
				}
				break
			}

			if ComparePos(ent.Pos, pos) > 0 {
				pos.Suffix = ent.Pos.Suffix
				pos.Offset = ent.Pos.Offset
				if err1 := p.cp.save(pos); err1 != nil {
					log.Errorf("save position %+v error %v", pos, err1)
				}
			}
		}

		if err != nil || len(entities) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
	}
}
