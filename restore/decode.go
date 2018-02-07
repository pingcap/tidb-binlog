package restore

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/juju/errors"
)

var (
	binlogMagic uint32 = 471532804
	crcTable           = crc32.MakeTable(crc32.Castagnoli)
)

//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// | magic word (4 byte)| Size (8 byte, len(payload)) |    payload    |  crc  |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
func readBinlog(br *bufio.Reader) ([]byte, error) {
	// read and chekc magic number
	magicNum, err := readInt32(br)
	if errors.Cause(err) == io.EOF {
		return nil, io.EOF
	}
	if err := checkMagic(magicNum); err != nil {
		return nil, errors.Trace(err)
	}
	// read payload length
	size, err := readInt64(br)
	if err != nil {
		if errors.Cause(err) == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, errors.Trace(err)
	}
	// size+4 = len(payload)+len(crc)
	data := make([]byte, size+4)
	// read payload+crc
	if _, err = io.ReadFull(br, data); err != nil {
		if errors.Cause(err) == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, errors.Trace(err)
	}
	payload := data[:size]
	// crc32 check
	entryCrc := binary.LittleEndian.Uint32(data[size:])
	crc := crc32.Checksum(payload, crcTable)
	if crc != entryCrc {
		return nil, errors.Errorf("expected crc32 %v but got %v", entryCrc, crc)
	}
	return payload, nil
}

func checkMagic(magicNum uint32) error {
	if magicNum != binlogMagic {
		return errors.Errorf("expected magic %d but got %d", binlogMagic, magicNum)
	}
	return nil
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, errors.Trace(err)
}

func readInt32(r io.Reader) (uint32, error) {
	var n uint32
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, errors.Trace(err)
}
