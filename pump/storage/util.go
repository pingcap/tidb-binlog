// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/dustin/go-humanize"
	"github.com/pingcap/errors"
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

// HumanizeBytes is used for humanize configure
type HumanizeBytes uint64

// Uint64 return bytes
func (b HumanizeBytes) Uint64() uint64 {
	return uint64(b)
}

// UnmarshalText implements UnmarshalText
func (b *HumanizeBytes) UnmarshalText(text []byte) error {
	var err error

	if len(text) == 0 {
		*b = 0
		return nil
	}

	n, err := humanize.ParseBytes(string(text))
	if err != nil {
		return errors.Annotatef(err, "test: %s", string(text))
	}

	*b = HumanizeBytes(n)
	return nil
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
