package pump

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

var magic uint32 = 471532804

//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  | magic word (4 byte)| Size (8 byte, len(payload)) |    payload    |  crc  |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type encoder struct {
	bw io.Writer
}

func newEncoder(w io.Writer) *encoder {
	return &encoder{
		bw: w,
	}
}

func (e *encoder) encode(payload []byte) error {
	var err error

	crc := crc32.Checksum(payload, crcTable)

	// length count crc + payload
	length := len(payload)

	// size is length of magic + size + crc + payload
	size := length + 16
	data := make([]byte, size)

	binary.LittleEndian.PutUint32(data[:4], magic)
	binary.LittleEndian.PutUint64(data[4:12], uint64(length))
	copy(data[12:size-4], payload)
	binary.LittleEndian.PutUint32(data[size-4:], crc)

	size, err = e.bw.Write(data)
	if err != nil {
		return err
	}

	return nil
}
