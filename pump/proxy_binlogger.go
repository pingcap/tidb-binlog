package pump

import (
	"fmt"
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

func newProxy(nodeID string, master, replicate Binlogger, cp *checkPoint) Binlogger {
	p := &Proxy{
		nodeID:    nodeID,
		master:    master,
		replicate: replicate,
		cp:        cp,
	}

	log.Infof("proxy checkpoint %+v", cp.pos())
	p.ctx, p.cancel = context.WithCancel(context.Background())

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.sync()
	}()

	return p
}

// ReadFrom implements ReadFrom WriteTail interface
func (p *Proxy) ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error) {
	return p.master.ReadFrom(from, nums)
}

// WriteTail implements Binlogger WriteTail interface
func (p *Proxy) WriteTail(entity *binlog.Entity) (int64, error) {
	p.Lock()
	defer p.Unlock()

	n, err := p.master.WriteTail(entity)
	return n, errors.Trace(err)
}

func (p *Proxy) AsyncWriteTail(entity *binlog.Entity, cb callback) {
	offset, err := p.WriteTail(entity)
	if cb != nil {
		cb(offset, err)
	}
}

// Close closes the binlogger
func (p *Proxy) Close() error {
	p.Lock()
	defer p.Unlock()

	var pos binlog.Pos
	for {
		// wait to write all binlogs into slave
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

		log.Infof("proxy wait to write all binlogs into kafka, now read at %+v, the latest pos %+v", pos, latestFilePos)
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
func (p *Proxy) Walk(ctx context.Context, from binlog.Pos, sendBinlog func(entity *binlog.Entity) error) error {
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

// Rotate implements the Binlogger interface.
func (p *Proxy) Rotate(int64) error {
	return nil
}

func (p *Proxy) updatePosition(readPos binlog.Pos, pos binlog.Pos) (binlog.Pos, error) {
	if ComparePos(readPos, pos) > 0 {
		// always return new position
		if err := p.cp.save(readPos, false); err != nil {
			log.Errorf("save position %+v error %v", readPos, err)
			return readPos, errors.Trace(err)
		}
		checkpointGauge.WithLabelValues("current").Set(posToFloat(&readPos))
		return readPos, nil
	}

	return pos, nil
}

func (p *Proxy) sync() {
	writePos := p.cp.pos()

	var saveMut sync.Mutex

	syncBinlog := func(entity *binlog.Entity) error {
		if GlobalConfig.enableDebug {
			printDebugBinlog(entity, entity.Pos)
		}

		writePos = entity.Pos

		p.replicate.AsyncWriteTail(entity, func(_ int64, err error) {
			if err != nil {
				panic(err)
			}

			saveMut.Lock()
			defer saveMut.Unlock()
			pos := p.cp.pos()
			_, err = p.updatePosition(entity.Pos, pos)
			if err != nil {
				log.Error(err)
			}
		})

		return nil
	}

	for {
		select {
		case <-p.ctx.Done():
			log.Info("context cancel - sycner exists")
			return
		default:
			err := p.master.Walk(p.ctx, writePos, syncBinlog)
			if err != nil {
				log.Errorf("master walk error %v", errors.ErrorStack(err))
			}
			// FIXME avoid this latency...
			time.Sleep(time.Second)
		}
	}
}

func printDebugBinlog(entity *binlog.Entity, pos binlog.Pos) {
	str := fmt.Sprintf("\n========== [proxy debug] update position from %+v to %+v\n", pos, entity.Pos)

	b := new(binlog.Binlog)
	err := b.Unmarshal(entity.Payload)
	if err != nil {
		str = str + fmt.Sprintf("unmarshal payload error %v \n", err)
	} else {
		str = str + fmt.Sprintf("binlog start ts %d \n", b.StartTs)
		str = str + fmt.Sprintf("binlog commit ts %d \n", b.CommitTs)
		str = str + fmt.Sprintf("binlog Type ts %d \n", b.GetTp())
	}

	str = str + "=================================================================\n"
	log.Warning(str)
}
