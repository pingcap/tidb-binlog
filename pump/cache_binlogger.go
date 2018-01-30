package pump

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	binlog "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
)

const maxCacheBinlogSize = 4 * 1024 * 1024 * 1024

type cacheBinloger struct {
	cache map[binlog.Pos][]byte

	currentSize int
	sync.RWMutex
}

func createCacheBinlogger() Binlogger {
	return &cacheBinloger{
		cache: make(map[binlog.Pos][]byte),
	}
}

// ReadFrom implements ReadFrom WriteTail interface
func (c *cacheBinloger) ReadFrom(from binlog.Pos, nums int32) ([]binlog.Entity, error) {
	c.RLock()
	defer c.RUnlock()

	if c.currentSize == 0 {
		return nil, nil
	}

	payload, ok := c.cache[from]
	if !ok {
		return []binlog.Entity{}, nil
	}

	ent := binlog.Entity{
		Payload: payload,
	}
	return []binlog.Entity{ent}, nil
}

// WriteTail implements Binlogger WriteTail interface
func (c *cacheBinloger) WriteTail(payload []byte) error {
	// for concurrency write
	c.Lock()
	c.Unlock()

	if len(payload) == 0 || c.currentSize >= maxCacheBinlogSize {
		return nil
	}

	c.cache[latestFilePos] = payload
	c.currentSize += len(payload)

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

		payload, ok := c.cache[from]
		if !ok {
			return from, nil
		}

		ent := binlog.Entity{
			Pos:     from,
			Payload: payload,
		}

		err := sendBinlog(ent)
		if err != nil {
			return from, errors.Trace(err)
		}

		delete(c.cache, from)
		from.Offset += int64(len(payload) + 16)
		c.currentSize -= len(payload)
		time.Sleep(time.Second)
	}
}

// Close implements Binlogger Close interface
func (c *cacheBinloger) Close() error {
	return nil
}

// GC implements Binlogger GC interface
func (c *cacheBinloger) GC(days time.Duration, pos binlog.Pos) {}
