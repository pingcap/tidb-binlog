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

package reparo

import (
	"bytes"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

type testDecodeSuite struct{}

var _ = check.Suite(&testDecodeSuite{})

func (s *testDecodeSuite) TestDecode(c *check.C) {
	binlog := &pb.Binlog{
		Tp:       pb.BinlogType_DDL,
		CommitTs: 1000000000,
	}

	data, err := binlog.Marshal()
	c.Assert(err, check.IsNil)

	data = binlogfile.Encode(data)
	reader := bytes.NewReader(data)

	decodeBinlog, n, err := Decode(reader)
	c.Assert(err, check.IsNil)
	c.Assert(int(n), check.Equals, len(data))
	c.Assert(decodeBinlog, check.DeepEquals, binlog)
}
