package storage

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
)

/*
log file := record* footer
record :=
  magic: uint32   // magic number of a record start
  length: uint64  // payload 长度
  checksum: uint32   // checksum of payload
  payload:  uint8[length]    // binlog 数据
footer :=
  maxTS: uint64     // the max ts of all binlog in this log file, so we can check if we can safe delete the file when gc accroding to ts
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
		return n, errors.Trace(err)
	}

	n, err = writer.Write(payload)
	if err != nil {
		return int(headerLength) + n, errors.Trace(err)
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
		err = errors.Trace(err)
		return
	}

	info, err := fd.Stat()
	if err != nil {
		err = errors.Trace(err)
		return
	}

	lf = &logFile{
		fid:  fid,
		fd:   fd,
		path: name,
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

	if lf.end == false {
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

	return errors.Trace(lf.sync())
}

func (lf *logFile) close() {
	lf.fd.Close()
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

	log.Debugf("offset: %d after read header record: %+v", offset-headerLength, record)

	if record.magic != recordMagic {
		return nil, ErrWrongMagic
	}

	record.payload = make([]byte, record.length)

	_, err = lf.fd.ReadAt(record.payload, offset)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	if record.isValid() == false {
		err = errors.New("checksum mismatch")
		return
	}

	return
}

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

	for offset < size {
		r, err := lf.readRecord(offset)
		if err != nil {
			return err
		}
		err = fn(valuePointer{Fid: lf.fid, Offset: offset}, r)
		offset += r.recordLength()
		if err != nil {
			return err
		}
	}

	return nil
}
