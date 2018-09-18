package storage

import (
	"encoding/binary"
	"sync/atomic"
)

var tsKeyPrefix = []byte("ts:")

func decodeTSKey(key []byte) int64 {
	// check bound
	_ = key[len(tsKeyPrefix)+8-1]

	return int64(binary.BigEndian.Uint64(key[len(tsKeyPrefix):]))
}

func encodeTSKey(ts int64) []byte {
	buf := make([]byte, 8+len(tsKeyPrefix))
	copy(buf, tsKeyPrefix)

	b := buf[len(tsKeyPrefix):]

	binary.BigEndian.PutUint64(b, uint64(ts))

	return buf
}

// test helper
type memOracle struct {
	ts int64
}

func newMemOracle() *memOracle {
	return &memOracle{
		ts: 0,
	}
}

func (o *memOracle) getTS() int64 {
	return atomic.AddInt64(&o.ts, 1)
}
