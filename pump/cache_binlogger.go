package pump

import (
	"sync"
	"time"

	queue "github.com/golang-collections/collections/queue"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	binlog "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
)

//const maxCacheBinlogSize = 4 * 1024 * 1024 * 1024
const maxCacheBinlogSize = 56

type cacheBinloger struct {
	qu *queue.Queue

	currentSize int
	sync.RWMutex
}

func createCacheBinlogger() Binlogger {
	return &cacheBinloger{
		qu: queue.New(),
	}
}

// ReadFrom implements ReadFrom WriteTail interface
func (c *cacheBinloger) ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error) {
	c.RLock()
	defer c.RUnlock()

	if c.currentSize == 0 {
		return nil, nil
	}

	entity := c.peek()

	if nums > 0 {
		c.deQueue()
	}

	return []binlog.Entity{entity}, nil
}

// WriteTail implements Binlogger WriteTail interface
func (c *cacheBinloger) WriteTail(payload []byte) error {
	// for concurrency write
	c.Lock()
	c.Unlock()

	if !isAvailable || len(payload) == 0 || c.currentSize >= maxCacheBinlogSize {
		return nil
	}

	entity := binlog.Entity{
		Pos:     latestFilePos,
		Payload: payload,
	}

	c.enQueue(entity)

	return nil
}

// Walk reads binlog from the "from" position and sends binlogs in the streaming way
func (c *cacheBinloger) Walk(ctx context.Context, from binlog.Pos, sendBinlog func(entity binlog.Entity) error) (binlog.Pos, error) {
	for {
		select {
		case <-ctx.Done():
			log.Warningf("slave Walk Done !")
			return from, nil
		default:
		}

		ent := c.peek()
		if len(ent.Payload) == 0 {
			return from, nil
		}

		log.Infof("slave send binlog pos %v len %v", ent.Pos, len(ent.Payload))
		err := sendBinlog(ent)
		if err != nil {
			return from, errors.Trace(err)
		}

		c.deQueue()
	}
}

// Close implements Binlogger Close interface
func (c *cacheBinloger) Close() error {
	return nil
}

// GC implements Binlogger GC interface
func (c *cacheBinloger) GC(days time.Duration, pos binlog.Pos) {}

func (c *cacheBinloger) peek() binlog.Entity {
	ent := c.qu.Peek()
	if ent == nil {
		return binlog.Entity{}
	}

	return ent.(binlog.Entity)
}

func (c *cacheBinloger) enQueue(ent binlog.Entity) {
	c.qu.Enqueue(ent)
	c.currentSize += len(ent.Payload)

	if c.currentSize >= maxCacheBinlogSize {
		isAvailable = false
	}
}

func (c *cacheBinloger) deQueue() binlog.Entity {
	ent := c.qu.Dequeue().(binlog.Entity)
	c.currentSize -= len(ent.Payload)

	return ent
}
