package pump

import (
	"encoding/binary"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/slicer"
	binlog "github.com/pingcap/tipb/go-binlog"
)

var _ = Suite(&testSlicerSuite{})

type testSlicerSuite struct{}

func (s *testSlicerSuite) TestGenerate(c *C) {
	sli := NewKafkaSlicer("test", 0)

	var (
		pos = binlog.Pos{
			Suffix: 1,
			Offset: 20,
		}
		duplicate = 4
		basisData = []byte("binlogtest")
		testData  = make([]byte, 0, 4*len(basisData))
		checksum  = []byte("hash")
		messageID = []byte(BinlogSliceMessageID(pos))
	)

	for i := 0; i < duplicate; i++ {
		testData = append(testData, basisData...)
	}

	entity := &binlog.Entity{
		Pos:      pos,
		Payload:  testData,
		Checksum: checksum,
	}

	messages, err := sli.Generate(entity)
	c.Assert(err, IsNil)
	c.Assert(messages, HasLen, 1)
	c.Assert(messages[0].Headers, HasLen, 0)

	GlobalConfig.EnableBinlogSlice = true
	messages, err = sli.Generate(entity)
	c.Assert(err, IsNil)
	c.Assert(messages, HasLen, 1)
	c.Assert(messages[0].Headers, HasLen, 0)

	GlobalConfig.SlicesSize = len(basisData)
	messages, err = sli.Generate(entity)
	c.Assert(err, IsNil)
	c.Assert(messages, HasLen, duplicate)

	Nos := make([]bool, duplicate)
	for i := 0; i < duplicate-1; i++ {
		c.Assert(messages[i].Headers, HasLen, 3)
		for _, header := range messages[i].Headers {
			switch string(header.Key) {
			case string(slicer.No):
				Nos[int(binary.LittleEndian.Uint32(header.Value))] = true
			case string(slicer.Total):
				c.Assert(binary.LittleEndian.Uint32(header.Value), Equals, uint32(duplicate))
			case string(slicer.MessageID):
				c.Assert(header.Value, DeepEquals, messageID)
			default:
				c.Fatalf("unsupport header %s", header.Key)
			}
		}
	}

	c.Assert(messages[duplicate-1].Headers, HasLen, 4)
	for _, header := range messages[duplicate-1].Headers {
		switch string(header.Key) {
		case string(slicer.No):
			Nos[int(binary.LittleEndian.Uint32(header.Value))] = true
		case string(slicer.Total):
			c.Assert(binary.LittleEndian.Uint32(header.Value), Equals, uint32(duplicate))
		case string(slicer.MessageID):
			c.Assert(header.Value, DeepEquals, messageID)
		case string(slicer.Checksum):
			c.Assert(header.Value, DeepEquals, checksum)
		default:
			c.Fatalf("unsupport header %s", header.Key)
		}
	}

	for _, exist := range Nos {
		c.Assert(exist, IsTrue)
	}
}
