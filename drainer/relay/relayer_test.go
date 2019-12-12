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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
)

func TestRelayer(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRelayerSuite{})

type testRelayerSuite struct {
	translator.BinlogGenrator
}

func (r *testRelayerSuite) TestCreate(c *C) {
	relayer, err := NewRelayer("", binlogfile.SegmentSizeBytes, nil)
	c.Assert(relayer, IsNil)
	c.Assert(err, IsNil)

	dir := c.MkDir()
	relayer, err = NewRelayer(dir, binlogfile.SegmentSizeBytes, nil)
	c.Assert(relayer, NotNil)
	c.Assert(err, IsNil)
	relayer.Close()

	relayer, err = NewRelayer("/", binlogfile.SegmentSizeBytes, nil)
	c.Assert(err, NotNil)
}

func (r *testRelayerSuite) TestWriteBinlog(c *C) {
	dir := c.MkDir()
	relayer, err := NewRelayer(dir, binlogfile.SegmentSizeBytes, nil)
	c.Assert(relayer, NotNil)
	c.Assert(err, IsNil)
	defer relayer.Close()

	r.SetDDL()
	pos1, err := relayer.WriteBinlog(r.Schema, r.Table, r.TiBinlog, nil)
	c.Assert(err, IsNil)
	c.Assert(pos1.Suffix, Equals, uint64(0))
	c.Assert(pos1.Offset, Greater, int64(0))

	r.SetInsert(c)
	pos2, err := relayer.WriteBinlog(r.Schema, r.Table, r.TiBinlog, nil)
	c.Assert(err, IsNil)
	c.Assert(pos2.Suffix, Equals, uint64(0))
	c.Assert(pos2.Offset, Greater, pos1.Offset)
}

func (r *testRelayerSuite) TestGCBinlog(c *C) {
	dir := c.MkDir()
	relayer, err := NewRelayer(dir, 10, nil)
	c.Assert(relayer, NotNil)
	c.Assert(err, IsNil)
	defer relayer.Close()

	r.SetDDL()
	pos1, err := relayer.WriteBinlog(r.Schema, r.Table, r.TiBinlog, nil)
	c.Assert(err, IsNil)
	checkRelayLogNumber(c, dir, 2)
	relayer.GCBinlog(pos1)
	checkRelayLogNumber(c, dir, 2)

	r.SetDDL()
	pos2, err := relayer.WriteBinlog(r.Schema, r.Table, r.TiBinlog, nil)
	c.Assert(err, IsNil)
	checkRelayLogNumber(c, dir, 3)
	relayer.GCBinlog(pos2)
	checkRelayLogNumber(c, dir, 2)
}

func checkRelayLogNumber(c *C, dir string, expectedNumber int) {
	names, err := binlogfile.ReadBinlogNames(dir)
	c.Assert(err, IsNil)
	c.Assert(len(names), Equals, expectedNumber)
}
