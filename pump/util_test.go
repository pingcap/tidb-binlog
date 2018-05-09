package pump

import (
	"io"
	"io/ioutil"
	"os"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	binlog "github.com/pingcap/tipb/go-binlog"
)

func (t *testPumpServerSuite) TestSeekNextBinlog(c *C) {
	f, err := ioutil.TempFile(os.TempDir(), "testOffset")
	c.Assert(err, IsNil)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	encoder := newEncoder(f, compress.CompressionNone)
	_, err = encoder.Encode([]byte("testOffset"))
	c.Assert(err, IsNil)

	testCase := make([]byte, 2048)
	for i := 0; i < 2048; i++ {
		testCase[i] = 'a'
	}
	_, err = f.Write(testCase)
	c.Assert(err, IsNil)

	_, err = encoder.Encode([]byte("testOffset"))
	c.Assert(err, IsNil)
	_, err = f.Write(testCase)
	c.Assert(err, IsNil)

	offset, err := seekNextBinlog(f, 10)
	c.Assert(offset, Equals, int64(2064))
	c.Assert(err, IsNil)

	_, err = seekNextBinlog(f, 2070)
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
			_, err = bl.WriteTail([]byte("binlogtest"))
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
