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

package binlogfile

import (
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/pingcap/errors"
)

var magic uint32 = 471532804

//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  | magic word (4 byte)| Size (8 byte, len(payload)) |    payload    |  crc  |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

// Encoder is an interface wraps basic Encode method which encodes payload and write it, and returns offset.
type Encoder interface {
	Encode(payload []byte) (int64, error)
}

type encoder struct {
	bw     io.Writer
	offset int64
}

// NewEncoder creates a Encoder instance
func NewEncoder(w io.Writer, initOffset int64) Encoder {
	return &encoder{
		bw:     w,
		offset: initOffset,
	}
}

// Encode implements interface of Encoder
func (e *encoder) Encode(payload []byte) (int64, error) {
	data := Encode(payload)
	_, err := e.bw.Write(data)
	if err != nil {
		return 0, errors.Trace(err)
	}

	e.offset += int64(len(data))

	return e.offset, nil
}

// Encode encodes the payload
func Encode(payload []byte) []byte {
	crc := crc32.Checksum(payload, crcTable)

	// length count payload
	length := len(payload)

	// size is length of magic + size + crc + payload
	size := length + 16
	data := make([]byte, size)

	binary.LittleEndian.PutUint32(data[:4], magic)
	binary.LittleEndian.PutUint64(data[4:12], uint64(length))
	copy(data[12:size-4], payload)
	binary.LittleEndian.PutUint32(data[size-4:], crc)
	return data
}
