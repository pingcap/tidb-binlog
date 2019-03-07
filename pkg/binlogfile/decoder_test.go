package binlogfile

import (
	"bytes"
	"io"
	"testing"

	"github.com/pingcap/check"
)

func Test(t *testing.T) { check.TestingT(t) }

type decoderSuite struct{}

var _ = check.Suite(&decoderSuite{})

func (s *decoderSuite) TestDecode(c *check.C) {
	buf := new(bytes.Buffer)

	// write one record
	_, err := buf.Write(Encode([]byte("payload")))
	c.Assert(err, check.IsNil)

	decoder := NewDecoder(buf, 0)

	// read the record back and check
	payload, _, err := decoder.Decode()
	c.Assert(err, check.IsNil)
	c.Assert(payload, check.BytesEquals, []byte("payload"))

	// only one byte will reach io.ErrUnexpectedEOF error
	err = buf.WriteByte(1)
	c.Assert(err, check.IsNil)
	_, _, err = decoder.Decode()
	c.Assert(err, check.Equals, io.ErrUnexpectedEOF)
}
