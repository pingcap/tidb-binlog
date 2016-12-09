package cistern

import (
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/juju/errors"
	pb "github.com/pingcap/tipb/go-binlog"
)

// the defaultBufferSize must be exponential power of 2
const defaultBufferSize int64 = 16 << 10

// Buffer is a ring buffer for binlog
type Buffer struct {
	startCursor int64
	endCursor   int64
	size        int64
	mask        int64
	buf         []pb.Entity
}

// NewBuffer returns a simple ring buffer that is for binlog
func NewBuffer() *Buffer {
	return &Buffer{
		size: defaultBufferSize,
		mask: defaultBufferSize - 1,
		buf:  make([]pb.Entity, defaultBufferSize),
	}
}

// Store stores the binlog and forward the end cursor
func (b *Buffer) Store(ctx context.Context, data pb.Entity) error {
	for {
		select {
		case <-ctx.Done():
			return errors.New("context was canceled")
		default:
			start := b.GetStartCursor()
			end := b.endCursor
			if end-start < b.size {
				b.buf[end&b.mask] = data
				atomic.AddInt64(&b.endCursor, 1)
				return nil
			}

			time.Sleep(retryTimeout)
		}
	}
}

// Get returns the first binlog in the buffer
func (b *Buffer) Get() pb.Entity {
	return b.buf[b.startCursor&b.mask]
}

// Next forwards the start cursor by one step
func (b *Buffer) Next() {
	atomic.AddInt64(&b.startCursor, 1)
}

// GetStartCursor returns the start cursor
func (b *Buffer) GetStartCursor() int64 {
	return atomic.LoadInt64(&b.startCursor)
}

// GetEndCursor returns the end cursor
func (b *Buffer) GetEndCursor() int64 {
	return atomic.LoadInt64(&b.endCursor)
}

// GetByIndex returns the binlog which locate the index
func (b *Buffer) GetByIndex(index int64) pb.Entity {
	return b.buf[index&b.mask]
}
