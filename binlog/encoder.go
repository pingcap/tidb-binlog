package binlog

import (
	"io"
	"sync"
	"encoding/binary"

	"github.com/pingcap/tidb-binlog/binlog/scheme"
)

type encoder struct {
	mu 		sync.Mutex
	bw 		io.Writer

	buf       	[]byte
	uint64buf 	[]byte
	uint32buf	[]byte
}

func newEncoder(w io.Writer) *encoder {
	return &encoder{
		bw:        w,
		buf:       make([]byte, 1024*1024),
		uint64buf: make([]byte, 8),
	}
}

func (e *encoder) encode(ent *scheme.Entry) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var (
		data []byte
		err  error
		n    int
	)

	if ent.SizeOfEntry() > len(e.buf) {
		data, err = ent.Marshal()
		if err != nil {
			return err
		}
	} else {
		n, err = ent.MarshalTo(e.buf)
		if err != nil {
			return err
		}
		data = e.buf[:n]
	}

	lenField, padBytes := encodeFrameSize(len(data))
	if err = writeUint64(e.bw, lenField, e.uint64buf); err != nil {
		return err
	}

	if padBytes != 0 {
		data = append(data, make([]byte, padBytes)...)
	}
	_, err = e.bw.Write(data)
	return err
}

func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)
	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		lenField |= uint64(0x80|padBytes) << 56
	}
	return
}

func writeUint64(w io.Writer, n uint64, buf []byte) error {
	binary.LittleEndian.PutUint64(buf, n)
	_, err := w.Write(buf)
	return err
}
