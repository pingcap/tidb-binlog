package pump

import (
	"io/ioutil"
	"os"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/compress"
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

	_, err = f.Write([]byte("aaa"))
	c.Assert(err, IsNil)

	_, err = encoder.Encode([]byte("testOffset"))
	c.Assert(err, IsNil)
	_, err = f.Write([]byte("aaa"))
	c.Assert(err, IsNil)

	offset, err := seekNextBinlog(f, 10)
	c.Assert(offset, Equals, 12)
	c.Assert(err, IsNil)

	_, err = seekNextBinlog(f, 15)
	c.Assert(err, isNil)
}
