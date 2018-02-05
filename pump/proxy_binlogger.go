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
			if ComparePos(entities[0].Pos, latestFilePos) >= 0 {
				log.Info("complete sync, read end of binlog file")
				break
			}
			log.Infof("close read pos %+v vs latest file position %+v", entities[0].Pos, latestFilePos)
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

func (p *Proxy) sync() {
	p.wg.Add(1)
	defer p.wg.Done()

	var err error
	pos := p.cp.pos()
	syncBinlog := func(entity binlog.Entity) error {
		err = p.replicate.WriteTail(entity.Payload)
		if err != nil {
			log.Errorf("write binlog to replicate error %v payload length %d", err, len(entity.Payload))
			return errors.Trace(err)
		}

		if ComparePos(entity.Pos, pos) > 0 {
			pos.Suffix = entity.Pos.Suffix
			pos.Offset = entity.Pos.Offset
			if err1 := p.cp.save(pos); err1 != nil {
				log.Errorf("save position %+v error %v", pos, err1)
				return errors.Trace(err)
			}
		}

		return nil
	}

	for {
		select {
		case <-p.ctx.Done():
			log.Info("context cancel - sycner exists")
			return
		default:
			_, err = p.master.Walk(p.ctx, pos, syncBinlog)
			if err != nil {
				log.Errorf("master walk error %v", err)
				time.Sleep(time.Second)
			}
		}
	}
}
