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
// if master has error,  switch master and cache
type Proxy struct {
	sync.RWMutex
	wg sync.WaitGroup

	master    Binlogger
	replicate Binlogger
	cache     Binlogger
	cp        *checkPoint

	enableSwitch bool

	ctx    context.Context
	cancel context.CancelFunc
}

func newProxy(master, replicate, cache Binlogger, cp *checkPoint, enableProxySwitch bool) Binlogger {
	p := &Proxy{
		master:    master,
		replicate: replicate,
		cache:     cache,
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
	} else {
		err = p.cache.WriteTail(payload)
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
				log.Info("complete sync, read end of binlog file")
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

// Walk reads binlog from the "from" position and sends binlogs in the streaming way
func (p *Proxy) Walk(ctx context.Context, from binlog.Pos, sendBinlog func(entity binlog.Entity) error) (binlog.Pos, error) {
	return p.master.Walk(ctx, from, sendBinlog)
}

// GC recycles the old binlog file
func (p *Proxy) GC(days time.Duration, pos binlog.Pos) {
	p.master.GC(days, p.cp.pos())
}

func (p *Proxy) checkSavePos(entPos binlog.Pos, pos binlog.Pos) error {
	if ComparePos(entPos, pos) > 0 {
		if err := p.cp.save(entPos); err != nil {
			log.Errorf("save position %+v error %v", entPos, err)
			return errors.Trace(err)
		}
	}

	return nil
}

func (p *Proxy) sync() {
	p.wg.Add(1)
	defer p.wg.Done()

	var err error
	pos := p.cp.pos()
	syncBinlog := func(entity binlog.Entity) error {
		err = p.replicate.WriteTail(entity.Payload)
		if err != nil {
			log.Errorf("write binlog to replicate error %v", err)
			return errors.Trace(err)
		}

		err = p.checkSavePos(entity.Pos, p.cp.pos())
		if err != nil {
			return errors.Trace(err)
		}

		return nil
	}

	for {
		select {
		case <-p.ctx.Done():
			log.Info("context cancel - sycner exists")
			return
		default:
			ent, err := p.cache.ReadFrom(pos, 0)
			if err != nil {
				log.Errorf("read binlog entity from cache error %v", err)
				continue
			}
			if len(ent) != 0 {
				pos, err = p.cache.Walk(p.ctx, pos, syncBinlog)
				if err != nil {
					log.Errorf("cache walk error %v", err)
					time.Sleep(time.Second)
				}
			} else {
				pos = p.cp.pos()
				pos, err = p.master.Walk(p.ctx, pos, syncBinlog)
				if err != nil {
					log.Errorf("master walk error %v", err)
					time.Sleep(time.Second)
				}
			}
		}
	}
}
