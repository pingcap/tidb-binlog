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

package relay

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	tb "github.com/pingcap/tipb/go-binlog"
)

var _ = Suite(&testRelayerSuite{})

type testRelayerSuite struct{
	translator.BinlogGenrator
}

func newRelayer(c *C) Relayer {
	dir := c.MkDir()
	relayer, err := NewRelayer(dir, binlogfile.SegmentSizeBytes, nil)
	c.Assert(relayer, NotNil)
	c.Assert(err, IsNil)
	return relayer
}

func clearRelayLogs(c *C, relayer Relayer) {
	pos := tb.Pos{999999, 0}
	relayer.GCBinlog(pos)
}

func (r *testRelayerSuite) TestCreate(c *C) {
	relayer, err := NewRelayer("", binlogfile.SegmentSizeBytes, nil)
	c.Assert(relayer, IsNil)
	c.Assert(err, IsNil)

	dir := c.MkDir()
	relayer, err = NewRelayer(dir, binlogfile.SegmentSizeBytes, nil)
	c.Assert(relayer, NotNil)
	c.Assert(err, IsNil)

	relayer, err = NewRelayer("/", binlogfile.SegmentSizeBytes, nil)
	c.Assert(err, NotNil)
}

func (r *testRelayerSuite) TestWriteBinlog(c *C) {
	relayer := newRelayer(c)
	clearRelayLogs(c, relayer)

	r.SetDDL()
	pos, err := relayer.WriteBinlog(r.Schema, r.Table, r.TiBinlog, nil)
	c.Assert(err, IsNil)
	c.Assert(pos.Suffix, Equals, 0)
}