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

package binlogfile

import (
	"bytes"
	"io"
	"testing"

	"github.com/pingcap/check"
)

func Test(t *testing.T) { check.TestingT(t) }

type decoderSuite struct{}

var _ = check.Suite(&decoderSuite{})

func (s *decoderSuite) TestDecode(c *check.C) {
	buf := new(bytes.Buffer)

	// write one record
	_, err := buf.Write(Encode([]byte("payload")))
	c.Assert(err, check.IsNil)

	decoder := NewDecoder(buf, 0)

	// read the record back and check
	payload, _, err := decoder.Decode()
	c.Assert(err, check.IsNil)
	c.Assert(payload, check.BytesEquals, []byte("payload"))

	// only one byte will reach io.ErrUnexpectedEOF error
	err = buf.WriteByte(1)
	c.Assert(err, check.IsNil)
	_, _, err = decoder.Decode()
	c.Assert(err, check.Equals, io.ErrUnexpectedEOF)
}
