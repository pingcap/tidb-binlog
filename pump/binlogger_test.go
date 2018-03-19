package pump

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	bf "github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	"github.com/pingcap/tipb/go-binlog"
)

var _ = Suite(&testBinloggerSuite{})

type testBinloggerSuite struct{}

func (s *testBinloggerSuite) TestCreate(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binloggertest")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	bl, err := CreateBinlogger(dir, compress.CompressionNone)
	c.Assert(err, IsNil)
	defer CloseBinlogger(bl)

	b, ok := bl.(*binlogger)
	c.Assert(ok, IsTrue)
	c.Assert(path.Base(b.file.Name()), Equals, bf.BinlogName(0))

	bl.Close()

	_, err = CreateBinlogger(dir, compress.CompressionNone)
	c.Assert(err, Equals, os.ErrExist)
}

func (s *testBinloggerSuite) TestOpenForWrite(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binloggertest")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	bl, err := OpenBinlogger(dir, compress.CompressionNone)
	c.Assert(errors.Cause(err), Equals, bf.ErrFileNotFound)

	bl, err = CreateBinlogger(dir, compress.CompressionNone)
	c.Assert(err, IsNil)

	b, ok := bl.(*binlogger)
	c.Assert(ok, IsTrue)
	b.rotate()

	_, err = bl.WriteTail([]byte("binlogtest"))
	c.Assert(err, IsNil)
	bl.Close()

	bl, err = OpenBinlogger(dir, compress.CompressionNone)
	c.Assert(err, IsNil)

	b, ok = bl.(*binlogger)
	curFile := b.file
	c.Assert(ok, IsTrue)
	c.Assert(path.Base(curFile.Name()), Equals, bf.BinlogName(1))
	latestPos := &binlog.Pos{Suffix: 1}
	c.Assert(latestPos.Suffix, Equals, uint64(1))

	curOffset, err := curFile.Seek(0, os.SEEK_CUR)
	c.Assert(err, IsNil)

	_, err = b.WriteTail([]byte("binlogtest"))
	c.Assert(err, IsNil)

	nowOffset, err := curFile.Seek(0, os.SEEK_CUR)
	c.Assert(err, IsNil)
	c.Assert(nowOffset, Equals, curOffset+26)

	bl.Close()
}

func (s *testBinloggerSuite) TestRotateFile(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binloggertest")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	bl, err := CreateBinlogger(dir, compress.CompressionNone)
	c.Assert(err, IsNil)

	ent := []byte("binlogtest")

	_, err = bl.WriteTail(ent)
	c.Assert(err, IsNil)

	b, ok := bl.(*binlogger)
	c.Assert(ok, IsTrue)

	err = b.rotate()
	c.Assert(err, IsNil)
	c.Assert(path.Base(b.file.Name()), Equals, bf.BinlogName(1))

	_, err = bl.WriteTail(ent)
	c.Assert(err, IsNil)

	bl.Close()

	bl, err = OpenBinlogger(dir, compress.CompressionNone)
	c.Assert(err, IsNil)

	binlogs, err := bl.ReadFrom(binlog.Pos{}, 1)
	c.Assert(err, IsNil)
	c.Assert(binlogs, HasLen, 1)
	c.Assert(binlogs[0].Pos, DeepEquals, binlog.Pos{Offset: 26})
	c.Assert(binlogs[0].Payload, BytesEquals, []byte("binlogtest"))

	binlogs, err = bl.ReadFrom(binlog.Pos{Suffix: 1, Offset: 0}, 1)
	c.Assert(err, IsNil)
	c.Assert(binlogs, HasLen, 1)
	c.Assert(binlogs[0].Pos, DeepEquals, binlog.Pos{Suffix: 1, Offset: 26})
	c.Assert(binlogs[0].Payload, BytesEquals, []byte("binlogtest"))
	bl.Close()
}

func (s *testBinloggerSuite) TestRead(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binloggertest")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	bl, err := CreateBinlogger(dir, compress.CompressionNone)
	c.Assert(err, IsNil)
	defer bl.Close()

	b, ok := bl.(*binlogger)
	c.Assert(ok, IsTrue)

	for i := 0; i < 10; i++ {
		for i := 0; i < 20; i++ {
			_, err = bl.WriteTail([]byte("binlogtest"))
			c.Assert(err, IsNil)
		}

		c.Assert(b.rotate(), IsNil)
	}

	ents, err := bl.ReadFrom(binlog.Pos{}, 11)
	c.Assert(err, IsNil)
	c.Assert(ents, HasLen, 11)
	c.Assert(ents[10].Pos, DeepEquals, binlog.Pos{Offset: 286})

	ents, err = bl.ReadFrom(binlog.Pos{Suffix: 0, Offset: 286}, 11)
	c.Assert(err, IsNil)
	c.Assert(ents, HasLen, 11)
	c.Assert(ents[10].Pos, DeepEquals, binlog.Pos{Suffix: 1, Offset: 52})

	ents, err = bl.ReadFrom(binlog.Pos{Suffix: 1, Offset: 52}, 18)
	c.Assert(err, IsNil)
	c.Assert(ents, HasLen, 18)
	c.Assert(ents[17].Pos, DeepEquals, binlog.Pos{Suffix: 1, Offset: 26 * 20})

	ents, err = bl.ReadFrom(binlog.Pos{Offset: 26, Suffix: 5}, 20)
	c.Assert(err, IsNil)
	c.Assert(ents, HasLen, 20)
	c.Assert(ents[19].Pos, Equals, binlog.Pos{Offset: 26, Suffix: 6})
}

func (s *testBinloggerSuite) TestCourruption(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binloggertest")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	bl, err := CreateBinlogger(dir, compress.CompressionNone)
	c.Assert(err, IsNil)
	defer bl.Close()

	b, ok := bl.(*binlogger)
	c.Assert(ok, IsTrue)

	for i := 0; i < 3; i++ {
		for i := 0; i < 4; i++ {
			_, err = bl.WriteTail([]byte("binlogtest"))
			c.Assert(err, IsNil)
		}

		c.Assert(b.rotate(), IsNil)
	}

	file := path.Join(dir, bf.BinlogName(1))
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE, 0600)
	c.Assert(err, IsNil)

	err = f.Truncate(73)
	c.Assert(err, IsNil)

	err = f.Close()
	c.Assert(err, IsNil)

	ents, err := bl.ReadFrom(binlog.Pos{Suffix: 1, Offset: 26}, 4)
	c.Assert(ents, HasLen, 1)
	c.Assert(err, Equals, io.ErrUnexpectedEOF)
}

func (s *testBinloggerSuite) TestGC(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binloggertest")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	bl, err := CreateBinlogger(dir, compress.CompressionNone)
	c.Assert(err, IsNil)
	defer CloseBinlogger(bl)

	b, ok := bl.(*binlogger)
	c.Assert(ok, IsTrue)
	b.rotate()

	time.Sleep(10 * time.Millisecond)
	b.GC(time.Millisecond, binlog.Pos{})

	names, err := bf.ReadBinlogNames(b.dir)
	c.Assert(err, IsNil)
	c.Assert(names, HasLen, 1)
	c.Assert(names[0], Equals, bf.BinlogName(1))
}
