package pump

import (
	"encoding/binary"
	"hash/crc32"
	"io"

	pb "github.com/pingcap/tidb-binlog/proto"
)

var magic uint32 = 471532804

//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  | magic word (4 byte)| Size (8 byte, len(crc+payload)) | crc (4 byte) |  Payload  |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type encoder struct {
	bw io.Writer
}

func newEncoder(w io.Writer) *encoder {
	return &encoder{
		bw: w,
	}
}

func (e *encoder) encode(ent *pb.Binlog) error {
	var err error

	crc := crc32.Checksum(ent.Payload, crcTable)

	// size count crc + payload
	size := len(ent.Payload) + 4

	// data length is magic + size + crc + payload
	data := make([]byte, size+12)

	binary.LittleEndian.PutUint32(data[:4], magic)
	binary.LittleEndian.PutUint64(data[4:12], uint64(size))
	binary.LittleEndian.PutUint32(data[12:16], crc)
	for i := 0; i < len(ent.Payload); i++ {
		data[i+16] = ent.Payload[i]
	}

	size, err = e.bw.Write(data)
	if err != nil {
		return err
	}

	return nil
}
