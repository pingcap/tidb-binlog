package binlog

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/binlog/scheme"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testBinlogSuite{})

type testBinlogSuite struct{}

func (s *testBinlogSuite) TestCreate(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binlogtest")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	b, err := Create(dir)
	c.Assert(err, IsNil)

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

	_, err = Create(dir)
	if err != os.ErrExist {
		c.Fatalf("err = %v, but want %v", err, os.ErrExist)
	}

	if err == nil {
		b.Close()
	}
}

func (s *testBinlogSuite) TestOpenForWrite(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binlogtest")
	c.Assert(err, IsNil)

	defer os.RemoveAll(dir)

	b, err := Create(dir)
	c.Assert(err, IsNil)

	b.rotate()
	err = b.Write([]scheme.Entry{scheme.Entry{Payload: []byte("binlogtest")}})
	c.Assert(err, IsNil)

	b.Close()

	b, err = Open(dir)
	c.Assert(err, IsNil)

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

	err = b.Write([]scheme.Entry{scheme.Entry{Payload: []byte("binlogtest")}})
	c.Assert(err, IsNil)

	nowOffset, err := curFile.Seek(0, os.SEEK_CUR)
	c.Assert(err, IsNil)

	if nowOffset != curOffset+32 {
		c.Fatalf("now offset is %d, want %d", nowOffset, curOffset+32)
	}

	b.Close()
}

func (s *testBinlogSuite) TestRotateFile(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binlogtest")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	b, err := Create(dir)
	c.Assert(err, IsNil)

	ent := scheme.Entry{
		Payload: []byte("binlogtest"),
	}

	err = b.Write([]scheme.Entry{ent})
	c.Assert(err, IsNil)

	err = b.rotate()
	c.Assert(err, IsNil)

	binlogName := fileName(1)
	if g := path.Base(b.file.Name()); g != binlogName {
		c.Fatalf("name = %+v, want %+v", g, binlogName)
	}

	err = b.Write([]scheme.Entry{ent})
	c.Assert(err, IsNil)

	b.Close()

	b, err = OpenForRead(dir)
	c.Assert(err, IsNil)

	f1, err := b.Read(1, scheme.BinlogPosition{})
	c.Assert(err, IsNil)
	b.Close()

	b, err = OpenForRead(dir)
	c.Assert(err, IsNil)

	f2, err := b.Read(1, scheme.BinlogPosition{Suffix: 1, Offset: 0})
	c.Assert(err, IsNil)
	b.Close()

	if len(f1) != 1 {
		c.Fatalf("nums of read entry = %d, but want 1", len(f1))
	}

	if len(f2) != 1 {
		c.Fatalf("nums of read entry = %d, but want 1", len(f2))
	}

	if f1[0].Offset.Suffix != 0 || f1[0].Offset.Offset != 16 {
		c.Fatalf("entry 1 offset is err, index = %d, offset = %d", f1[0].Offset.Suffix, f1[0].Offset.Offset)
	}

	if f2[0].Offset.Suffix != 1 || f2[0].Offset.Offset != 16 {
		c.Fatalf("entry 2 offset is err, index = %d, offset = %d", f2[0].Offset.Suffix, f2[0].Offset.Offset)
	}

	if string(f1[0].Payload) != "binlogtest" {
		c.Fatalf("entry 1  is err, payload = %s", string(f1[0].Payload))
	}
	if string(f2[0].Payload) != "binlogtest" {
		c.Fatalf("entry 2 is err, payload = %s", string(f2[0].Payload))
	}
}

func (s *testBinlogSuite) TestRead(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binlogtest")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	b, err := Create(dir)
	c.Assert(err, IsNil)
	b.Close()

	var es []scheme.Entry
	for i := 0; i < 10; i++ {
		es = append(es, scheme.Entry{
			Payload: []byte("binlogtest"),
		})
	}

	for i := 0; i < 20; i++ {
		b, err = Open(dir)
		c.Assert(err, IsNil)

		err = b.Write(es)
		c.Assert(err, IsNil)

		if i%2 == 1 {
			err = b.rotate()
			c.Assert(err, IsNil)
		}
		b.Close()
	}

	b2, err := OpenForRead(dir)
	c.Assert(err, IsNil)
	defer b2.Close()

	ents, err := b2.Read(11, scheme.BinlogPosition{})
	c.Assert(err, IsNil)
	if ents[10].Offset.Suffix != 0 || ents[10].Offset.Offset != 336 {
		c.Fatalf("last index read = %d, want %d; offset = %d, want %d", ents[10].Offset.Suffix, 0, ents[10].Offset.Offset, 336)
	}

	ents, err = b2.Read(11, scheme.BinlogPosition{Suffix: 0, Offset: 368})
	c.Assert(err, IsNil)
	if ents[10].Offset.Suffix != 1 || ents[10].Offset.Offset != 48 {
		c.Fatalf("last index read = %d, want %d; offset = %d, want %d", ents[10].Offset.Suffix, 1, ents[10].Offset.Offset, 48)
	}

	ents, err = b2.Read(18, scheme.BinlogPosition{Suffix: 1, Offset: 80})
	c.Assert(err, IsNil)
	if ents[17].Offset.Suffix != 1 || ents[17].Offset.Offset != 32*19+16 {
		c.Fatalf("last index read = %d, want %d; offset = %d, want %d", ents[17].Offset.Suffix, 1, ents[17].Offset.Offset, 32*19+16)
	}

	b3, err := OpenForRead(dir)
	c.Assert(err, IsNil)
	defer b3.Close()

	ents, err = b3.Read(20, scheme.BinlogPosition{Offset: 48, Suffix: 5})
	c.Assert(err, IsNil)
	if ents[19].Offset.Suffix != 6 || ents[19].Offset.Offset != 16 {
		c.Fatalf("last index read = %d, want %d; offset = %d, want %d", ents[19].Offset.Suffix, 6, ents[19].Offset.Offset, 0)
	}
}

func (s *testBinlogSuite) TestCourruption(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binlogtest")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	b, err := Create(dir)
	c.Assert(err, IsNil)
	b.Close()

	var es []scheme.Entry
	for i := 0; i < 4; i++ {
		es = append(es, scheme.Entry{
			Payload: []byte("binlogtest"),
		})
	}

	for i := 0; i < 3; i++ {
		b, err := Open(dir)
		c.Assert(err, IsNil)

		err = b.Write(es)
		c.Assert(err, IsNil)

		err = b.rotate()
		c.Assert(err, IsNil)

		b.Close()
	}

	cfile1 := path.Join(dir, fileName(1))
	f1, err := os.OpenFile(cfile1, os.O_WRONLY|os.O_CREATE, 0600)
	c.Assert(err, IsNil)

	err = f1.Truncate(81)

	err = f1.Close()
	c.Assert(err, IsNil)

	b1, err := OpenForRead(dir)
	c.Assert(err, IsNil)
	defer b1.Close()

	ents, err := b1.Read(4, scheme.BinlogPosition{Suffix: 1, Offset: 48})
	if err != io.ErrUnexpectedEOF || len(ents) != 1 {
		c.Fatalf("err = %v, want nil; count of ent = %d, want 1", err, len(ents))
	}
}
