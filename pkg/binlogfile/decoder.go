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
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/pingcap/errors"
)

// Decoder is an interface wraps basic Decode method which decode binlog.Entity into binlogBuffer.
type Decoder interface {
	Decode() (payload []byte, offset int64, err error)
}

type decoder struct {
	br     *bufio.Reader
	offset int64
}

// NewDecoder creates a new Decoder.
func NewDecoder(r io.Reader, initOffset int64) Decoder {
	reader := bufio.NewReader(r)

	return &decoder{
		br:     reader,
		offset: initOffset,
	}
}

// Decode implements the Decoder interface.
func (d *decoder) Decode() (payload []byte, offset int64, err error) {
	if d.br == nil {
		return nil, 0, io.EOF
	}

	var length int64
	payload, length, err = Decode(d.br)
	if err != nil {
		return
	}

	d.offset += int64(length)
	offset = d.offset

	return
}

// CheckMagic check weather the magicNum is right
func CheckMagic(mgicNum uint32) error {
	if mgicNum != magic {
		return ErrMagicMismatch
	}

	return nil
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}

func readInt32(r io.Reader) (uint32, error) {
	var n uint32
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}

// Decode return payload and bytes read from io.Reader
func Decode(r io.Reader) (payload []byte, length int64, err error) {
	// read and chekc magic number
	magicNum, err := readInt32(r)
	if err != nil {
		return
	}

	if err = CheckMagic(magicNum); err != nil {
		return nil, 0, errors.Trace(err)
	}

	// read payload length
	size, err := readInt64(r)
	if err != nil {
		if errors.Cause(err) == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return
	}

	// size+4 = len(payload)+len(crc)
	data := make([]byte, size+4)
	// read payload+crc
	if _, err = io.ReadFull(r, data); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return
	}
	payload = data[:size]

	// crc32 check
	entryCrc := binary.LittleEndian.Uint32(data[size:])
	crc := crc32.Checksum(payload, crcTable)
	if crc != entryCrc {
		return nil, 0, errors.Errorf("expected crc32 %v but got %v", entryCrc, crc)
	}

	// len(magic) + len(size) + len(payload) + len(crc)
	length = 4 + 8 + size + 4
	return payload, length, nil
}
