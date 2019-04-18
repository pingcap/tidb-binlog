package binlogfile

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
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
func NewDecoder(f *os.File, initOffset int64) (Decoder, error) {
	r, err := NewReaderFromFile(f)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewDecoderFromReader(r, initOffset), nil
}

// NewDecoderFromReader creates a new Decoder from io.reader.
func NewDecoderFromReader(r io.Reader, initOffset int64) Decoder {
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

// DecodeBinlog returns binlog and bytes read from io.Reader
func DecodeBinlog(r io.Reader) (*pb.Binlog, int64, error) {
	payload, length, err := Decode(r)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	binlog := &pb.Binlog{}
	err = binlog.Unmarshal(payload)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return binlog, length, nil
}

// NewReaderFromFile returns a reader from file.
func NewReaderFromFile(f *os.File) (r io.Reader, err error) {
	if compress.IsGzipCompressFile(f.Name()) {
		r, err = gzip.NewReader(f)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		r = bufio.NewReader(f)
	}

	return r, nil
}
