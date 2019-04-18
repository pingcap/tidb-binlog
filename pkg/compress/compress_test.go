package compress

import (
	"compress/gzip"
	"io"
	"os"
	"path"
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testCompressSuite{})

type testCompressSuite struct{}

func (t *testCompressSuite) TestIsGzipCompressFile(c *C) {
	testCases := []struct {
		filename         string
		isComrepressFile bool
	}{
		{
			"binlog-0001.tar.gz",
			true,
		},
		{
			"binlog-0001",
			false,
		},
		{
			"binlog-0001.gz",
			false,
		},
	}

	for _, testCase := range testCases {
		isComrepressFile := IsCompressFile(testCase.filename)
		c.Assert(isComrepressFile, Equals, testCase.isComrepressFile)

		isComrepressFile := IsGzipCompressFile(testCase.filename)
		c.Assert(isComrepressFile, Equals, testCase.isComrepressFile)
	}
}

func (t *testCompressSuite) TestCompressFile(c *C) {
	dir := c.MkDir()
	filename := path.Join(dir, "compress-binlog")
	file, err := os.Create(filename)
	c.Assert(err, IsNil)

	message := []byte("hello tidb")
	_, err = file.Write(message)
	c.Assert(err, IsNil)
	file.Close()

	compressaFileName, err := CompressGZIPFile(filename)
	c.Assert(err, IsNil)

	f, err := os.OpenFile(compressaFileName, os.O_RDONLY, 0600)
	c.Assert(err, IsNil)
	reader, err := gzip.NewReader(f)
	c.Assert(err, IsNil)

	data := make([]byte, 10)
	_, err = io.ReadFull(reader, data)
	c.Assert(err, IsNil)
	c.Assert(data, DeepEquals, message)
}
