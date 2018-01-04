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
	slave     Binlogger
	cp        *checkPoint

	enableSwitch bool

	ctx    context.Context
	cancel context.CancelFunc
}

func newProxy(master, slave, replicate Binlogger, cp *checkPoint, enableProxySwitch bool) Binlogger {
	p := &Proxy{
		master:    master,
		slave:     slave,
		replicate: replicate,
		cp:        cp,

		enableSwitch: enableProxySwitch,
	}

	go p.switchMS()
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

	var err error
	if p.master.IsAvailable() {
		err = p.master.WriteTail(payload)
	} else {
		err = errors.New("binlogger is not avaliable")
	}
	if err == nil {
		return nil
	}

	if !p.enableSwitch {
		return errors.Trace(err)
	}

	return errors.Trace(p.slave.WriteTail(payload))
}

// IsAvailable implements Binlogger IsAvailable interface
func (p *Proxy) IsAvailable() bool {
	return true
}

// MarkAvailable implements binlogger MarkAvailable interface
func (p *Proxy) MarkAvailable() {}

// Close closes the binlogger
func (p *Proxy) Close() error {
	p.Lock()
	defer p.Unlock()
	
	log.Warningf("start close pump")
	pos := p.cp.pos()
	entities, err := p.master.ReadFrom(pos, 1000)
	if err != nil {
		log.Errorf("read binlogs from master error %v", err)
	}

	for len(entities) != 0 {
		log.Warningf("len(entities) is %v", len(entities))
		err = p.sync()
		if err != nil {
			log.Errorf("sync error %v", err)
		}
		
		pos = p.cp.pos()
		entities, err = p.master.ReadFrom(pos, 1000)
	}

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
	p.wg.Wait()

	return nil
}

// GC recycles the old binlog file
func (p *Proxy) GC(days time.Duration) {
	p.master.GC(days)
}

func (p *Proxy) switchMS() {
	ticker := time.NewTicker(switchDetectInterval)
	p.wg.Add(1)
	defer func() {
		ticker.Stop()
		p.wg.Done()
	}()

	for {
		select {
		case <-p.ctx.Done():
			log.Info("context cancel - switch manager exists")
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
		p.RUnlock()
		if err != nil {
			return errors.Trace(err)
		}
		if len(entities) == 0 {
			p.master.MarkAvailable()
			return nil
		}

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

func (p *Proxy) sync() error {
	p.wg.Add(1)
	defer p.wg.Done()

	pos := p.cp.pos()
	for {
		select {
		case <-p.ctx.Done():
			log.Info("context cancel - switch manager exists")
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
					log.Info("context cancel - switch manager exists")
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
