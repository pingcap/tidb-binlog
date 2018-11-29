package pump

import (
	"bytes"
	"io"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	binlog "github.com/pingcap/tipb/go-binlog"
)

type decoderSuite struct{}

var _ = check.Suite(&decoderSuite{})

func (s *decoderSuite) TestDecode(c *check.C) {
	buf := new(bytes.Buffer)

	// write one record
	payloadData, err := encode([]byte("payload"), compress.CompressionNone, compress.DefaultCompressLevel)
	c.Assert(err, check.IsNil)

	_, err = buf.Write(payloadData)
	c.Assert(err, check.IsNil)

	var ent binlog.Entity
	binlogBuffer := new(binlogBuffer)

	decoder := NewDecoder(binlog.Pos{}, buf)

	// read the record back and check
	err = decoder.Decode(&ent, binlogBuffer)
	c.Assert(err, check.IsNil)
	c.Assert(ent.Payload, check.BytesEquals, []byte("payload"))

	// only one byte will reach io.ErrUnexpectedEOF error
	err = buf.WriteByte(1)
	c.Assert(err, check.IsNil)
	err = decoder.Decode(&ent, binlogBuffer)
	c.Assert(err, check.Equals, io.ErrUnexpectedEOF)
}
