package repora

import (
	"io"
	"os"
	"path"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

type testReadSuite struct{}

var _ = check.Suite(&testReadSuite{})

func writeBinlogsInDir(dir string, c *check.C) (binlogs []*pb.Binlog) {
	// generate binlog with increasing commit ts for test
	var nextTS int64 = 1
	getPbBinLog := func() *pb.Binlog {
		binlog := &pb.Binlog{
			CommitTs: nextTS,
			Tp:       pb.BinlogType_DDL,
			DdlQuery: []byte("create database test"),
		}
		nextTS++
		return binlog
	}

	for index := 0; index < 10; index++ {
		// create the file to write binlog
		filename := path.Join(dir, binlogfile.BinlogName(uint64(index)))
		file, err := os.Create(filename)
		c.Assert(err, check.IsNil)

		// write index + 1 number binlogs in the file
		for j := 0; j < index+1; j++ {
			binlog := getPbBinLog()
			binlogData, err := binlog.Marshal()
			c.Assert(err, check.IsNil)

			binlogs = append(binlogs, binlog)
			_, err = file.Write(binlogfile.Encode(binlogData))
			c.Assert(err, check.IsNil)
		}

		file.Close()
	}

	return binlogs
}

func (s *testReadSuite) TestReader(c *check.C) {
	dir := c.MkDir()

	binlogs := writeBinlogsInDir(dir, c)

	// read back all binlogs in directory
	var readBackBinlogs []*pb.Binlog
	reader, err := newDirPbReader(dir)
	c.Assert(err, check.IsNil)

	for {
		binlog, err := reader.read()
		if len(readBackBinlogs) == len(binlogs) {
			c.Assert(err, check.Equals, io.EOF)
			break
		}

		c.Assert(err, check.IsNil)
		readBackBinlogs = append(readBackBinlogs, binlog)
	}

	c.Assert(readBackBinlogs, check.DeepEquals, binlogs)
}
