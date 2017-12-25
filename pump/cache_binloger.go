package pump

import (
	"sync"
	"time"

	queue "github.com/golang-collections/collections/queue"
	"github.com/ngaut/log"
	binlog "github.com/pingcap/tipb/go-binlog"
)

const maxCacheBinlogSize = 4 * 1024 * 1024 * 1024

type cacheBinloger struct {
	qu *queue.Queue

	currentSize int
	sync.RWMutex
}

func createCacheBinlogger() Binlogger {
	return &cacheBinloger{
		qu:          queue.New(),
		currentSize: 0,
	}
}

// ReadFrom implements ReadFrom WriteTail interface
func (c *cacheBinloger) ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error) {
	if c.currentSize == 0 {
		return nil, nil
	}

	entity := binlog.Entity{
		Payload: c.peek(),
	}
	if nums > 0 {
		c.deQueue()
	}

	return []binlog.Entity{entity}, nil
}

// WriteTail implements Binlogger WriteTail interface
func (c *cacheBinloger) WriteTail(payload []byte) error {
	// for concurrency write
	c.Lock()
	defer c.Unlock()

	defer func() {
		binlogCacheCounter.WithLabelValues("WriteCacheBinlog").Add(1)
		if c.currentSize >= maxCacheBinlogSize {
			log.Warningf("cache binlogger total size %d M is too large", c.currentSize/1024*1024)
			c.qu.Dequeue()
		}
	}()

	if len(payload) != 0 {
		c.qu.Enqueue(payload)
		c.currentSize += len(payload)
	}

	return nil
}

// IsAvailable implements Binlogger IsAvailable interface
func (c *cacheBinloger) IsAvailable() bool {
	return true
}

// MarkAvailable implements binlogger MarkAvailable interface
func (c *cacheBinloger) MarkAvailable() {}

// Close implements Binlogger Close interface
func (c *cacheBinloger) Close() error {
	return nil
}

// GC implements Binlogger GC interface
func (c *cacheBinloger) GC(days time.Duration) {}

func (c *cacheBinloger) peek() []byte {
	return c.qu.Peek().([]byte)
}

func (c *cacheBinloger) enQueue(payload []byte) {
	c.qu.Enqueue(payload)
}

func (c *cacheBinloger) deQueue() []byte {
	payload := c.qu.Dequeue().([]byte)
	c.currentSize -= len(payload)

	return payload
}
