// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package compress_test

import (
	"compress/gzip"
	"io"
	"os"
	"path"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/compress"
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
		isComrepressFile := compress.IsCompressFile(testCase.filename)
		c.Assert(isComrepressFile, Equals, testCase.isComrepressFile)

		isComrepressFile = compress.IsGzipCompressFile(testCase.filename)
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

	compressFileName := path.Join(dir, "compress-binlog.tar.gz")
	err = compress.CompressGZIPFile(filename, compressFileName)
	c.Assert(err, IsNil)

	f, err := os.OpenFile(compressFileName, os.O_RDONLY, 0600)
	c.Assert(err, IsNil)
	reader, err := gzip.NewReader(f)
	c.Assert(err, IsNil)

	data := make([]byte, len(message))
	_, err = io.ReadFull(reader, data)
	c.Assert(err, IsNil)
	c.Assert(data, DeepEquals, message)
}

func (t *testCompressSuite) TestGetCompressFileNameWithTS(c *C) {
	filename := compress.GetCompressFileNameWithTS("binlog-1", ".gzip", 123)
	c.Assert(filename, Equals, "binlog-1-123.gzip")
}

func (s *testCompressSuite) TestParseCompressCodec(c *C) {
	c.Assert(compress.ToCompressionCodec(""), Equals, compress.CompressionNone)
	c.Assert(compress.ToCompressionCodec("gzip"), Equals, compress.CompressionGZIP)
	c.Assert(compress.ToCompressionCodec("zstd"), Not(Equals), compress.CompressionGZIP)
}

