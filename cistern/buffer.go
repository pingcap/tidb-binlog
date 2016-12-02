package cistern

import (
	"sync/atomic"

	"golang.org/x/net/context"

	"github.com/juju/errors"
)

var defaultBufferSize = 100000

type buffer struct {
	startCursor int64
	endCursor   int64
	size        int64
	mask        int64
	buf         []*binlog.Binlog
}

func NewBuffer() *binlogBuffer {
	return &binlogBuffer{
		size: defaultBufferSize,
		mask: defaultBufferSize - 1,
		buf:  make([]*binlog.Binlog, defaultBufferSize),
	}
}

func (b *buffer) Store(ctx context.Context, data *binlog.Binlog) error {
	for {
		select {
		case <-p.ctx.Done():
			return errors.New("context was canceled")
		default:
			start := b.GetStartCursor()
			end := b.GetEndCursor()
			if end-start < b.size {
				b.buf[b.end&b.mask] = data
				atomic.AddInt64(&b.end, 1)
				return nil
			} else {
				time.Sleep(retryTimeout)
			}
		}
	}
}

func (b *buffer) Get(index int64) *binlog.Binlog {
	return b.buf[index&b.mask]
}

func (b *buffer) Next() {
	atomic.AddInt64(&b.start, 1)
}

func (b *buffer) GetStartCursor() int64 {
	return atomic.LoadInt64(&b.startCursor)
}

func (c *buffer) GetEndCursor() {
	return atomic.LoadInt64(&b.endCursor)
}
