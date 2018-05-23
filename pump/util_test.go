package pump

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	binlog "github.com/pingcap/tipb/go-binlog"
)

func (t *testPumpServerSuite) TestSeekBinlog(c *C) {
	f, err := ioutil.TempFile(os.TempDir(), "testOffset")
	c.Assert(err, IsNil)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	encoder := newEncoder(f, compress.CompressionNone)
	_, err = encoder.Encode(&binlog.Entity{Payload: []byte("binlogtest")})
	c.Assert(err, IsNil)

	testCase := make([]byte, 2048)
	binary.LittleEndian.PutUint32(testCase[:4], magic)
	for i := 4; i < 2048; i++ {
		testCase[i] = 'a'
	}
	_, err = f.Write(testCase)
	c.Assert(err, IsNil)

	_, err = encoder.Encode(&binlog.Entity{Payload: []byte("binlogtest")})
	c.Assert(err, IsNil)
	_, err = f.Write(testCase)
	c.Assert(err, IsNil)

	offset, err := seekBinlog(f, 10)
	c.Assert(offset, Equals, int64(26))
	c.Assert(err, IsNil)

	offset, err = seekBinlog(f, 26)
	c.Assert(offset, Equals, int64(26))
	c.Assert(err, IsNil)

	offset, err = seekBinlog(f, 27)
	c.Assert(offset, Equals, int64(2074))
	c.Assert(err, IsNil)

	offset, err = seekBinlog(f, 2080)
	c.Assert(offset, Equals, int64(2100))
	c.Assert(err, IsNil)

	offset, err = seekBinlog(f, 2100)
	c.Assert(offset, Equals, int64(2100))
	c.Assert(err, IsNil)

	_, err = seekBinlog(f, 2101)
	c.Assert(err, Equals, io.ErrUnexpectedEOF)
}

func (s *testBinloggerSuite) TestSkipCRCRead(c *C) {
	dir, err := ioutil.TempDir(os.TempDir(), "binloggertest")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	bl, err := OpenBinlogger(dir, compress.CompressionNone)
	c.Assert(err, IsNil)
	defer bl.Close()

	b, ok := bl.(*binlogger)
	c.Assert(ok, IsTrue)

	for i := 0; i < 10; i++ {
		for i := 0; i < 20; i++ {
			_, err = bl.WriteTail(&binlog.Entity{Payload: []byte("binlogtest")})
			c.Assert(err, IsNil)

			_, err = b.file.Write([]byte("test"))
			c.Assert(err, IsNil)
		}

		c.Assert(b.rotate(), IsNil)
	}

	ents, err := bl.ReadFrom(binlog.Pos{}, 11)
	c.Assert(err, IsNil)
	c.Assert(ents, HasLen, 11)
	c.Assert(ents[10].Pos, DeepEquals, binlog.Pos{Offset: 326})

	ents, err = bl.ReadFrom(binlog.Pos{Suffix: 0, Offset: 322}, 11)
	c.Assert(err, IsNil)
	c.Assert(ents, HasLen, 11)
	c.Assert(ents[10].Pos, DeepEquals, binlog.Pos{Suffix: 1, Offset: 56})

	ents, err = bl.ReadFrom(binlog.Pos{Suffix: 1, Offset: 56}, 18)
	c.Assert(err, IsNil)
	c.Assert(ents, HasLen, 18)
	c.Assert(ents[17].Pos, DeepEquals, binlog.Pos{Suffix: 1, Offset: 26*20 + 19*4})

	ents, err = bl.ReadFrom(binlog.Pos{Offset: 26, Suffix: 5}, 20)
	c.Assert(err, IsNil)
	c.Assert(ents, HasLen, 20)
	c.Assert(ents[19].Pos, Equals, binlog.Pos{Offset: 26, Suffix: 6})
}
