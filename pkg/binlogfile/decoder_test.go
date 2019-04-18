package binlogfile

import (
	"bytes"
	"io"
	"testing"

	"github.com/pingcap/check"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

func Test(t *testing.T) { check.TestingT(t) }

type decoderSuite struct{}

var _ = check.Suite(&decoderSuite{})

func (s *decoderSuite) TestDecode(c *check.C) {
	buf := new(bytes.Buffer)

	// write one record
	_, err := buf.Write(Encode([]byte("payload")))
	c.Assert(err, check.IsNil)

	decoder := NewDecoderFromReader(buf, 0)

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

func (s *decoderSuite) TestDecodeBinlog(c *check.C) {
	binlog := &pb.Binlog{
		Tp:       pb.BinlogType_DDL,
		CommitTs: 1000000000,
	}

	data, err := binlog.Marshal()
	c.Assert(err, check.IsNil)

	data = Encode(data)
	reader := bytes.NewReader(data)

	decodeBinlog, n, err := DecodeBinlog(reader)
	c.Assert(err, check.IsNil)
	c.Assert(int(n), check.Equals, len(data))
	c.Assert(decodeBinlog, check.DeepEquals, binlog)
}
