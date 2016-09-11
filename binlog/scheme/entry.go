package scheme

import (
	"encoding/binary"
	"errors"
)

type Entry struct {
	Payload []byte
	Type    uint32
	Crc     uint32
	Offset  BinlogPosition
}

var (
	ErrorFormat = errors.New("entry format is error")
)

func (ent *Entry) Unmarshal(b []byte, offset *BinlogPosition) error {
	length := len(b)

	if length < 8 {
		return ErrorFormat
	}

	ent.Type = binary.LittleEndian.Uint32(b[0:4])
	ent.Crc = binary.LittleEndian.Uint32(b[4:8])

	ent.Payload = b[8:]
	ent.Offset = BinlogPosition{
		Suffix: offset.Suffix,
		Offset: offset.Offset,
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
	binary.LittleEndian.PutUint32(data[:4], ent.Type)
	binary.LittleEndian.PutUint32(data[4:8], ent.Crc)
	for i := 0; i < len(ent.Payload); i++ {
		data[i+8] = ent.Payload[i]
	}

	return ent.SizeOfEntry(), nil
}

func (ent *Entry) SizeOfEntry() int {
	return len(ent.Payload) + 8
}
