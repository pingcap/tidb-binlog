package pump

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	. "github.com/pingcap/check"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testBinloggerSuite{})

type testBinloggerSuite struct{}

func (s *testBinloggerSuite) TestCreate(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binloggertest")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	binlog, err := CreateBinlogger(dir)
	c.Assert(err, IsNil)

	b, ok := binlog.(*binlogger)
	c.Assert(ok, IsTrue)

	curFile := b.file
	curOffset, err := curFile.Seek(0, os.SEEK_CUR)
	c.Assert(err, IsNil)

	fileInfo, err := curFile.Stat()
	c.Assert(err, IsNil)

	size := fileInfo.Size()

	if curOffset != size {
		c.Fatalf("offset = %d, but = %d", curOffset, size)
	}

	if g := path.Base(curFile.Name()); g != fileName(0) {
		c.Fatalf("name = %+v, want %+v", g, fileName(1))
	}

	b.Close()

	_, err = CreateBinlogger(dir)
	if err != os.ErrExist {
		c.Fatalf("err = %v, but want %v", err, os.ErrExist)
	}

	if err == nil {
		b.Close()
	}
}

func (s *testBinloggerSuite) TestOpenForWrite(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binloggertest")
	c.Assert(err, IsNil)

	defer os.RemoveAll(dir)

	binlog, err := CreateBinlogger(dir)
	c.Assert(err, IsNil)

	b, ok := binlog.(*binlogger)
	c.Assert(ok, IsTrue)

	b.rotate()
	err = b.WriteTail([]byte("binlogtest"))
	c.Assert(err, IsNil)

	b.Close()

	binlog, err = OpenBinlogger(dir)
	c.Assert(err, IsNil)

	b, ok = binlog.(*binlogger)
	c.Assert(ok, IsTrue)

	curFile := b.file
	curOffset, err := curFile.Seek(0, os.SEEK_CUR)
	c.Assert(err, IsNil)

	fileInfo, err := curFile.Stat()
	c.Assert(err, IsNil)

	size := fileInfo.Size()

	if curOffset != size {
		c.Errorf("offset = %d, but = %d", curOffset, size)
	}

	if g := path.Base(curFile.Name()); g != fileName(1) {
		c.Fatalf("name = %+v, want %+v", g, fileName(1))
	}

	err = b.WriteTail([]byte("binlogtest"))
	c.Assert(err, IsNil)

	nowOffset, err := curFile.Seek(0, os.SEEK_CUR)
	c.Assert(err, IsNil)

	if nowOffset != curOffset+26 {
		c.Fatalf("now offset is %d, want %d", nowOffset, curOffset+32)
	}

	b.Close()
}

func (s *testBinloggerSuite) TestRotateFile(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binloggertest")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	binlog, err := CreateBinlogger(dir)
	c.Assert(err, IsNil)

	ent := []byte("binlogtest")

	err = binlog.WriteTail(ent)
	c.Assert(err, IsNil)

	b, ok := binlog.(*binlogger)
	c.Assert(ok, IsTrue)

	err = b.rotate()
	c.Assert(err, IsNil)

	binlogName := fileName(1)
	if g := path.Base(b.file.Name()); g != binlogName {
		c.Fatalf("name = %+v, want %+v", g, binlogName)
	}

	err = b.WriteTail(ent)
	c.Assert(err, IsNil)

	b.Close()

	binlog, err = OpenBinlogger(dir)
	c.Assert(err, IsNil)

	f1, err := binlog.ReadFrom(pb.Pos{}, 1)
	c.Assert(err, IsNil)
	binlog.Close()

	binlog, err = OpenBinlogger(dir)
	c.Assert(err, IsNil)

	f2, err := binlog.ReadFrom(pb.Pos{Suffix: 1, Offset: 0}, 1)
	c.Assert(err, IsNil)
	binlog.Close()

	if len(f1) != 1 {
		c.Fatalf("nums of read entry = %d, but want 1", len(f1))
	}

	if len(f2) != 1 {
		c.Fatalf("nums of read entry = %d, but want 1", len(f2))
	}

	if f1[0].Pos.Suffix != 0 || f1[0].Pos.Offset != 0 {
		c.Fatalf("entry 1 offset is err, index = %d, offset = %d", f1[0].Pos.Suffix, f1[0].Pos.Offset)
	}

	if f2[0].Pos.Suffix != 1 || f2[0].Pos.Offset != 0 {
		c.Fatalf("entry 2 offset is err, index = %d, offset = %d", f2[0].Pos.Suffix, f2[0].Pos.Offset)
	}

	if string(f1[0].Payload) != "binlogtest" {
		c.Fatalf("entry 1  is err, payload = %s", string(f1[0].Payload))
	}
	if string(f2[0].Payload) != "binlogtest" {
		c.Fatalf("entry 2 is err, payload = %s", string(f2[0].Payload))
	}
}

func (s *testBinloggerSuite) TestRead(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binloggertest")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	b, err := CreateBinlogger(dir)
	c.Assert(err, IsNil)
	b.Close()

	for i := 0; i < 20; i++ {
		binlog, err := OpenBinlogger(dir)
		c.Assert(err, IsNil)

		for i := 0; i < 10; i++ {
			err = binlog.WriteTail([]byte("binlogtest"))
			c.Assert(err, IsNil)
		}

		b, ok := binlog.(*binlogger)
		c.Assert(ok, IsTrue)

		if i%2 == 1 {
			err = b.rotate()
			c.Assert(err, IsNil)
		}
		b.Close()
	}

	b2, err := OpenBinlogger(dir)
	c.Assert(err, IsNil)

	ents, err := b2.ReadFrom(pb.Pos{}, 11)
	c.Assert(err, IsNil)
	if ents[10].Pos.Suffix != 0 || ents[10].Pos.Offset != 260 {
		c.Fatalf("last index read = %d, want %d; offset = %d, want %d", ents[10].Pos.Suffix, 0, ents[10].Pos.Offset, 260)
	}

	ents, err = b2.ReadFrom(pb.Pos{Suffix: 0, Offset: 286}, 11)
	c.Assert(err, IsNil)
	if ents[10].Pos.Suffix != 1 || ents[10].Pos.Offset != 26 {
		c.Fatalf("last index read = %d, want %d; offset = %d, want %d", ents[10].Pos.Suffix, 1, ents[10].Pos.Offset, 26)
	}

	ents, err = b2.ReadFrom(pb.Pos{Suffix: 1, Offset: 52}, 18)
	c.Assert(err, IsNil)
	if ents[17].Pos.Suffix != 1 || ents[17].Pos.Offset != 26*19 {
		c.Fatalf("last index read = %d, want %d; offset = %d, want %d", ents[17].Pos.Suffix, 1, ents[17].Pos.Offset, 26*19)
	}
	b2.Close()

	b3, err := OpenBinlogger(dir)
	c.Assert(err, IsNil)
	defer b3.Close()

	ents, err = b3.ReadFrom(pb.Pos{Offset: 26, Suffix: 5}, 20)
	c.Assert(err, IsNil)
	if ents[19].Pos.Suffix != 6 || ents[19].Pos.Offset != 0 {
		c.Fatalf("last index read = %d, want %d; offset = %d, want %d", ents[19].Pos.Suffix, 6, ents[19].Pos.Offset, 0)
	}
}

func (s *testBinloggerSuite) TestCourruption(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binloggertest")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	b, err := CreateBinlogger(dir)
	c.Assert(err, IsNil)
	b.Close()

	for i := 0; i < 3; i++ {
		binlog, err := OpenBinlogger(dir)

		c.Assert(err, IsNil)

		for i := 0; i < 4; i++ {
			err = binlog.WriteTail([]byte("binlogtest"))
			c.Assert(err, IsNil)
		}

		b, ok := binlog.(*binlogger)
		c.Assert(ok, IsTrue)

		err = b.rotate()
		c.Assert(err, IsNil)

		b.Close()
	}

	cfile1 := path.Join(dir, fileName(1))
	f1, err := os.OpenFile(cfile1, os.O_WRONLY|os.O_CREATE, 0600)
	c.Assert(err, IsNil)

	err = f1.Truncate(73)

	err = f1.Close()
	c.Assert(err, IsNil)

	b1, err := OpenBinlogger(dir)
	c.Assert(err, IsNil)
	defer b1.Close()

	ents, err := b1.ReadFrom(pb.Pos{Suffix: 1, Offset: 26}, 4)
	if err != io.ErrUnexpectedEOF || len(ents) != 1 {
		c.Fatalf("err = %v, want nil; count of ent = %d, want 1", err, len(ents))
	}
}
