package scheme

import (
	"errors"

	"encoding/binary"
)

type Entry struct {
	CommitTs	uint64
	StartTs		uint64
	Size		uint64
	Payload		[]byte
	Offset		BinlogOffset
}

const magicByte = 0x19

var (
	ErrorFormat = errors.New("entry format is error")
)

func (ent *Entry) Unmarshal(b []byte, offset *BinlogOffset) error {
	length := len(b)

	if length <= 25 && b[0] != magicByte {
		return ErrorFormat
	}

	n, nerr := binary.Uvarint(b[17:25])
	if nerr <= 0  || int(n + 25) != length {
		return ErrorFormat
	}
	ent.Size = n

	n, nerr = binary.Uvarint(b[1:9])
	if nerr <= 0 {
                return ErrorFormat
        }
	ent.CommitTs = n

	n, nerr = binary.Uvarint(b[9:17])
        if nerr <= 0 {
                return ErrorFormat
        }
        ent.StartTs = n

	ent.Payload = b[25:]
	ent.Offset = BinlogOffset {
		Suffix: 	offset.Suffix,
		Offset:		offset.Offset,
	}
	
	return nil
}

func (ent *Entry) Marshal() (data []byte, err error) {
	size := ent.SizeOfEntry()
	data = make([]byte, size)
	n, err := ent.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (ent *Entry) MarshalTo(data []byte) (int, error) {
	data[0] = magicByte
	binary.PutUvarint(data[1:], ent.CommitTs)
	binary.PutUvarint(data[9:], ent.StartTs)
	binary.PutUvarint(data[17:], ent.Size)

	for i := 0; i < len(ent.Payload) && i+25 < len(data); i++ {
		data[i+25] = ent.Payload[i] 
	}

	return ent.SizeOfEntry(), nil
}

func (ent *Entry) SizeOfEntry() int {
	return 25 + len(ent.Payload)
}
