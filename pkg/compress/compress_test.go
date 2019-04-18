package compress_test

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/compress"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testCompressSuite{})

type testCompressSuite struct{}

func (s *testCompressSuite) TestParseCompressCodec(c *C) {
	c.Assert(compress.ToCompressionCodec(""), Equals, compress.CompressionNone)
	c.Assert(compress.ToCompressionCodec("gzip"), Equals, compress.CompressionGZIP)
	c.Assert(compress.ToCompressionCodec("zstd"), Not(Equals), compress.CompressionGZIP)
}

func (s *testCompressSuite) TestNoCompression(c *C) {
	data := bytes.Repeat([]byte("test.data"), 1000)
	payload, err := compress.Compress(data, compress.CompressionNone)
	c.Assert(err, IsNil)
	c.Assert(payload, BytesEquals, data)
}

func (s *testCompressSuite) TestGZIPCompression(c *C) {
	data := bytes.Repeat([]byte("test.data"), 1000)
	payload, err := compress.Compress(data, compress.CompressionGZIP)
	c.Assert(err, IsNil)
	c.Assert(len(payload), Less, len(data))

	// also check that decompressing the payload gets back the original data.
	decompressor, err := gzip.NewReader(bytes.NewReader(payload))
	c.Assert(err, IsNil)
	decompressed, err := ioutil.ReadAll(decompressor)
	c.Assert(err, IsNil)
	c.Assert(decompressed, BytesEquals, data)
}
