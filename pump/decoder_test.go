package pump

import (
	"bytes"
	"io"

	"github.com/pingcap/check"
	binlog "github.com/pingcap/tipb/go-binlog"
)

type decoderSuite struct{}

var _ = check.Suite(&decoderSuite{})

func (s *decoderSuite) TestDecode(c *check.C) {
	buf := new(bytes.Buffer)

	// write one record
	_, err := buf.Write(encode([]byte("payload")))
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
