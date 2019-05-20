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
	"bufio"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pb "github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
)

/*
log file := record* footer
record :=
  magic: uint32   // magic number of a record start
  length: uint64  // payload 长度
  checksum: uint32   // checksum of payload
  payload:  uint8[length]    // binlog 数据
footer :=
  maxTS: uint64     // the max ts of all binlog in this log file, so we can check if we can safe delete the file when gc according to ts
  fileEndMagic: uint32  // check if the file has a footer
*/

const recordMagic uint32 = 0x823a56e8
const fileEndMagic uint32 = 0x123ab922
const fileFooterLength int64 = 4 + 8 // fileEndMagic + maxTS
const headerLength int64 = 16        // 4 + 8 + 4 magic + length + checksum

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type logFile struct {
	fid  uint32
	path string

	// guard fd
	lock sync.RWMutex
	fd   *os.File
	// max ts of all binlog
	maxTS int64
	// end means the file has a footer and can not append record to it anymore
	end bool
	// Some corruption was detected.  "bytes" is the approximate number
	// of bytes dropped due to the corruption.
	// If "corruptionReporter" is non-NULL, it is notified whenever some data is
	// dropped due to a detected corruption when scan the log file.
	corruptionReporter func(bytes int, reason error)
}

// Record is the format in the log file
type Record struct {
	magic    uint32
	length   uint64
	checksum uint32
	payload  []byte
}

func (r *Record) recordLength() int64 {
	return headerLength + int64(len(r.payload))
}

func encodeRecord(writer io.Writer, payload []byte) (int, error) {
	header := make([]byte, headerLength)
	binary.LittleEndian.PutUint32(header, recordMagic)
	binary.LittleEndian.PutUint64(header[4:], uint64(len(payload)))

	checksum := crc32.Checksum(payload, crcTable)
	binary.LittleEndian.PutUint32(header[4+8:], checksum)

	n, err := writer.Write(header)
	if err != nil {
		return n, errors.Annotate(err, "write header failed")
	}

	n, err = writer.Write(payload)
	if err != nil {
		return int(headerLength) + n, errors.Annotate(err, "write payload failed")
	}

	return int(headerLength) + len(payload), nil
}

func (r *Record) readHeader(reader io.Reader) error {
	err := binary.Read(reader, binary.LittleEndian, &r.magic)
	if err != nil {
		return errors.Trace(err)
	}
	err = binary.Read(reader, binary.LittleEndian, &r.length)
	if err != nil {
		return errors.Trace(err)
	}
	err = binary.Read(reader, binary.LittleEndian, &r.checksum)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (r *Record) isValid() bool {
	return crc32.Checksum(r.payload, crcTable) == r.checksum
}

func newLogFile(fid uint32, name string) (lf *logFile, err error) {
	fd, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, errors.Trace(err)
	}

	info, err := fd.Stat()
	if err != nil {
		return nil, errors.Annotatef(err, "stat file %s failed", name)
	}

	logReporter := func(bytes int, reason error) {
		log.Warn("skip bytes", zap.Int("count", bytes), zap.String("reason", reason.Error()))
	}

	lf = &logFile{
		fid:                fid,
		fd:                 fd,
		path:               name,
		corruptionReporter: logReporter,
	}

	if info.Size() >= fileFooterLength {
		footer := make([]byte, fileFooterLength)
		_, err = fd.ReadAt(footer, info.Size()-fileFooterLength)
		if err != nil {
			err = errors.Trace(err)
			return
		}

		buf := bytes.NewReader(footer)

		var maxTS int64
		var magic uint32
		err = binary.Read(buf, binary.LittleEndian, &maxTS)
		if err != nil {
			return
		}
		err = binary.Read(buf, binary.LittleEndian, &magic)
		if err != nil {
			return
		}

		if magic == fileEndMagic {
			lf.end = true
			lf.maxTS = maxTS
		}
	}

	if !lf.end {
		err = lf.recover()
		if err != nil {
			err = errors.Trace(err)
			lf = nil
			return
		}
	}

	return
}

func (lf *logFile) updateMaxTS(ts int64) {
	if ts > lf.maxTS {
		lf.maxTS = ts
	}
}

// finalize write the footer to the file, then we never write this file anymore
func (lf *logFile) finalize() error {
	if lf.end {
		panic("unreachable")
	}
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, lf.maxTS)
	if err != nil {
		return errors.Trace(err)
	}

	err = binary.Write(buf, binary.LittleEndian, fileEndMagic)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = lf.fd.Write(buf.Bytes())
	if err != nil {
		return errors.Trace(err)
	}

	lf.end = true

	return errors.Trace(lf.fdatasync())
}

func (lf *logFile) close() error {
	return lf.fd.Close()
}

// recover scan all the record get the state like maxTS which only saved when the file is finalized
func (lf *logFile) recover() error {
	err := lf.scan(0, func(vp valuePointer, r *Record) error {
		// save ts in header to avoid this?
		b := new(pb.Binlog)
		err := b.Unmarshal(r.payload)
		if err != nil {
			return errors.Trace(err)
		}

		if b.CommitTs > lf.maxTS {
			lf.maxTS = b.CommitTs
		}

		if b.StartTs > lf.maxTS {
			lf.maxTS = b.StartTs
		}

		return nil
	})

	return errors.Trace(err)
}

// thread-safe to read record at specify offset
func (lf *logFile) readRecord(offset int64) (record *Record, err error) {
	header := make([]byte, headerLength)
	_, err = lf.fd.ReadAt(header, offset)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	offset += headerLength

	record = new(Record)
	err = record.readHeader(bytes.NewReader(header))
	if err != nil {
		err = errors.Trace(err)
		return
	}

	log.Debug("after read header", zap.Int64("offset", offset-headerLength), zap.Reflect("record", record))

	if record.magic != recordMagic {
		return nil, ErrWrongMagic
	}

	record.payload = make([]byte, record.length)

	_, err = lf.fd.ReadAt(record.payload, offset)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	if !record.isValid() {
		err = errors.New("checksum mismatch")
		return
	}

	return
}

func readRecord(reader io.Reader) (record *Record, err error) {
	record = new(Record)
	err = record.readHeader(reader)
	if err != nil {
		return nil, errors.Annotate(err, "read header failed")
	}

	if record.magic != recordMagic {
		return nil, ErrWrongMagic
	}

	// read directly to record.payload if record.length is less than some value
	// note the data may be corrupt and record.length is not the true value
	if record.length < (4 << 30) {
		record.payload = make([]byte, record.length)
		_, err = io.ReadFull(reader, record.payload)
		if err != nil {
			return nil, errors.Annotate(err, "read payload failed")
		}
	} else {
		buf := new(bytes.Buffer)
		_, err = io.CopyN(buf, reader, int64(record.length))
		if err != nil {
			err = errors.Trace(err)
			return
		}
		record.payload = buf.Bytes()
	}

	if !record.isValid() {
		return nil, errors.New("checksum mismatch")
	}

	return
}

// seek to the next record by recordMagic
// return the bytes skip, if err = nil then it 's seek to a record
func seekToNextRecord(reader *bufio.Reader) (bytes int, err error) {
	var buf []byte
	for {
		buf, err = reader.Peek(4)
		if err != nil {
			bytes += len(buf)
			err = errors.Trace(err)
			return
		}

		magic := binary.LittleEndian.Uint32(buf)
		if magic == recordMagic {
			return
		}

		reader.Discard(1)
		bytes++
	}
}

func (lf *logFile) reportCorruption(bytes int, err error) {
	if lf.corruptionReporter == nil {
		return
	}

	lf.corruptionReporter(bytes, err)
}

// scan is *Not* thread safe
func (lf *logFile) scan(startOffset int64, fn func(vp valuePointer, record *Record) error) error {
	info, err := lf.fd.Stat()
	if err != nil {
		return err
	}

	size := info.Size()

	if lf.end {
		size -= fileFooterLength
	}

	offset := startOffset
	var reader = bufio.NewReader(io.NewSectionReader(lf.fd, offset, size-offset))

	for offset < size {
		r, err := readRecord(reader)
		if err != nil {
			offset = offset + 1
			reader = bufio.NewReader(io.NewSectionReader(lf.fd, offset, size-offset))
			bytes, seekErr := seekToNextRecord(reader)
			if seekErr == nil {
				offset += int64(bytes)
				lf.reportCorruption(bytes+1, err)
				continue
			}

			// reach file end
			if errors.Cause(seekErr) == io.EOF {
				lf.reportCorruption(int(size)-int(offset), err)
				return nil
			}

			return errors.Trace(seekErr)
		}
		err = fn(valuePointer{Fid: lf.fid, Offset: offset}, r)
		if err != nil {
			return errors.Trace(err)
		}
		offset += r.recordLength()
	}

	return nil
}
