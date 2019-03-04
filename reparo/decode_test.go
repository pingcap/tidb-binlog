package repora

import (
	"bytes"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

type testDecodeSuite struct{}

var _ = check.Suite(&testDecodeSuite{})

func (s *testDecodeSuite) TestDecode(c *check.C) {
	binlog := &pb.Binlog{
		Tp:       pb.BinlogType_DDL,
		CommitTs: 1000000000,
	}

	data, err := binlog.Marshal()
	c.Assert(err, check.IsNil)

	data = binlogfile.Encode(data)
	reader := bytes.NewReader(data)

	decodeBinlog, n, err := Decode(reader)
	c.Assert(err, check.IsNil)
	c.Assert(int(n), check.Equals, len(data))
	c.Assert(decodeBinlog, check.DeepEquals, binlog)
}
