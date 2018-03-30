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
	wg     sync.WaitGroup
	nodeID string

	master    Binlogger
	replicate Binlogger
	cp        *checkPoint

	enableTolerant bool

	ctx    context.Context
	cancel context.CancelFunc
}

func newProxy(nodeID string, master, replicate Binlogger, cp *checkPoint, enableTolerant bool) Binlogger {
	p := &Proxy{
		nodeID:    nodeID,
		master:    master,
		replicate: replicate,
		cp:        cp,

		enableTolerant: enableTolerant,
	}

	p.ctx, p.cancel = context.WithCancel(context.Background())

	go p.sync()

	return p
}

// ReadFrom implements ReadFrom WriteTail interface
func (p *Proxy) ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error) {
	return p.master.ReadFrom(from, nums)
}

// WriteTail implements Binlogger WriteTail interface
func (p *Proxy) WriteTail(payload []byte) (int64, error) {
	p.Lock()
	defer p.Unlock()

	n, err := p.master.WriteTail(payload)
	if err != nil {
		lossBinlogCacheCounter.WithLabelValues(p.nodeID).Add(1)
		log.Errorf("write binlog error %v", err)
	}

	if p.enableTolerant {
		return 0, nil
	}

	return n, errors.Trace(err)
}

// Close closes the binlogger
func (p *Proxy) Close() error {
	p.Lock()
	defer p.Unlock()

	var pos binlog.Pos
	for {
		pos = p.cp.pos()
		entities, err := p.master.ReadFrom(pos, 1)
		if err == nil {
			if len(entities) == 0 {
				break
			}
		}
		if err != nil {
			log.Errorf("read binlogs from master in close error %v", err)
		}

		time.Sleep(time.Second)
	}

	err := p.cp.save(pos, true)
	if err != nil {
		log.Errorf("save position %+v error %v", pos, err)
	}
	log.Infof("complete sync, read end of binlog file position %v", pos)

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
func (p *Proxy) Walk(ctx context.Context, from binlog.Pos, sendBinlog func(entity binlog.Entity) error) error {
	return p.master.Walk(ctx, from, sendBinlog)
}

// GC recycles the old binlog file
func (p *Proxy) GC(days time.Duration, pos binlog.Pos) {
	p.master.GC(days, p.cp.pos())
}

// Name implements the Binlogger interface.
func (p *Proxy) Name() string {
	return "proxy"
}

func (p *Proxy) updatePosition(readPos binlog.Pos, pos binlog.Pos) (binlog.Pos, error) {
	if ComparePos(readPos, pos) > 0 {
		// always return new position
		if err := p.cp.save(readPos, false); err != nil {
			log.Errorf("save position %+v error %v", readPos, err)
			return readPos, errors.Trace(err)
		}
		return readPos, nil
	}

	return pos, nil
}

func (p *Proxy) sync() {
	p.wg.Add(1)
	defer p.wg.Done()

	pos := p.cp.pos()
	syncBinlog := func(entity binlog.Entity) error {
		_, err := p.replicate.WriteTail(entity.Payload)
		if err != nil {
			log.Errorf("write binlog to replicate error %v payload length %d", err, len(entity.Payload))
			return errors.Trace(err)
		}

		pos, err = p.updatePosition(entity.Pos, pos)
		return errors.Trace(err)
	}

	for {
		select {
		case <-p.ctx.Done():
			log.Info("context cancel - sycner exists")
			return
		default:
			err := p.master.Walk(p.ctx, pos, syncBinlog)
			if err != nil {
				log.Errorf("master walk error %v", errors.ErrorStack(err))
			}
			time.Sleep(time.Second)
		}
	}
}
