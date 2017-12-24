package pump

import (
	"sync"
	"time"

	queue "github.com/golang-collections/collections/queue"
	"github.com/ngaut/log"
	binlog "github.com/pingcap/tipb/go-binlog"
)

const maxCacheBinlogSize = 1024 * 1024

type cacheBinloger struct {
	qu *queue.Queue

	curSize int
	sync.RWMutex
}

func createCacheBinlogger() (Binlogger, error) {
	binlogger := &cacheBinloger{
		qu:      queue.New(),
		curSize: 0,
	}

	return binlogger, nil
}

// ReadFrom implements ReadFrom WriteTail interface
func (c *cacheBinloger) ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error) {
	return nil, nil
}

// WriteTail implements Binlogger WriteTail interface
func (c *cacheBinloger) WriteTail(payload []byte) error {
	// for concurrency write
	c.Lock()
	defer c.Unlock()

	var err error
	defer func() {
		var label string
		if err != nil {
			label = "fail"
		} else {
			label = "succ"
		}
		binlogCacheCounter.WithLabelValues("WriteBinlog", label).Add(1)

		if c.curSize >= maxCacheBinlogSize {
			log.Warningf("cache binlogger total size %d M is too large", c.curSize/1024*1024)
			c.qu.Dequeue()
		}

	}()

	if len(payload) != 0 {
		c.qu.Enqueue(payload)
		c.curSize += len(payload)
	}

	return nil
}

// WriteAvailable implements Binlogger WriteAvailable interface
func (c *cacheBinloger) WriteAvailable() bool {
	return true
}

// Close implements Binlogger Close interface
func (c *cacheBinloger) Close() error {
	return nil
}

// GC implements Binlogger GC interface
func (c *cacheBinloger) GC(days time.Duration) {}

func (c *cacheBinloger) getPeek() (payload interface{}) {
	return c.qu.Peek()
}

func (c *cacheBinloger) enQueue(payload interface{}) {
	c.qu.Enqueue(payload)
}

func (c *cacheBinloger) deQueue() (payload interface{}) {
	return c.qu.Dequeue()
}

func (c *cacheBinloger) getLen() int {
	return c.qu.Len()
}
