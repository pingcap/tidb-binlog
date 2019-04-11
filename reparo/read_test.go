package reparo

import (
	"io"
	"os"
	"path"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
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
		filename := path.Join(dir, binlogfile.BinlogName(uint64(index), nextTS))
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

func readAll(reader PbReader) (binlogs []*pb.Binlog, err error) {
	var binlog *pb.Binlog
	for {
		binlog, err = reader.read()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				err = nil
			}
			return
		}

		binlogs = append(binlogs, binlog)
	}
}

func (s *testReadSuite) TestReader(c *check.C) {
	dir := c.MkDir()

	binlogs := writeBinlogsInDir(dir, c)

	// read back all binlogs in directory
	var readBackBinlogs []*pb.Binlog
	reader, err := newDirPbReader(dir, 0, 0)
	c.Assert(err, check.IsNil)

	readBackBinlogs, err = readAll(reader)
	c.Assert(err, check.IsNil)
	c.Assert(readBackBinlogs, check.DeepEquals, binlogs)

	// we write the binlog with commit ts start at one(1,2,3,4...)
	for start := 1; start <= len(binlogs); start++ {
		for end := start; end <= len(binlogs); end++ {
			reader, err := newDirPbReader(dir, int64(start), int64(end))
			c.Assert(err, check.IsNil)

			readBackBinlogs, err = readAll(reader)
			c.Assert(err, check.IsNil)
			c.Assert(len(readBackBinlogs), check.Equals, end-start+1)
		}
	}

}
