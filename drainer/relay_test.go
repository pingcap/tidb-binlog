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

package drainer

import (
	"github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

type relaySuite struct{}

var _ = check.Suite(&relaySuite{})

type noOpLoader struct {
	input   chan *loader.Txn
	success chan *loader.Txn
}

// noOpLoader just return success for every input txn.
var _ loader.Loader = &noOpLoader{}

func newNoOpLoader() *noOpLoader {
	return &noOpLoader{
		input:   make(chan *loader.Txn, 1),
		success: make(chan *loader.Txn, 1),
	}
}

func (ld *noOpLoader) Run() error {
	for txn := range ld.input {
		ld.success <- txn
	}

	close(ld.success)
	return nil
}

func (ld *noOpLoader) Close() {
	close(ld.input)
}

func (ld *noOpLoader) Input() chan<- *loader.Txn {
	return ld.input
}

func (ld *noOpLoader) Successes() <-chan *loader.Txn {
	return ld.success
}

func (ld *noOpLoader) SetSafeMode(bool) {
}

func (ld *noOpLoader) GetSafeMode() bool {
	return false
}

var _ loader.Loader = &noOpLoader{}

func (s *relaySuite) TestFeedByRealyLog(c *check.C) {
	cp, err := checkpoint.NewFile(0 /* initialCommitTS */, c.MkDir()+"/checkpoint")
	c.Assert(err, check.IsNil)
	err = cp.Save(0, 0, checkpoint.StatusRunning)
	c.Assert(err, check.IsNil)
	c.Assert(cp.Status(), check.Equals, checkpoint.StatusRunning)

	ld := newNoOpLoader()

	// write some relay log
	gen := &translator.BinlogGenerator{}
	relayDir := c.MkDir()
	relayer, err := relay.NewRelayer(relayDir, binlogfile.SegmentSizeBytes, gen)
	c.Assert(err, check.IsNil)

	for i := 0; i < 10; i++ {
		gen.SetInsert(c)
		gen.TiBinlog.StartTs = int64(i)
		gen.TiBinlog.CommitTs = int64(i) * 10
		_, err = relayer.WriteBinlog(gen.Schema, gen.Table, gen.TiBinlog, gen.PV)
		c.Assert(err, check.IsNil)
	}

	relayer.Close()
	c.Assert(err, check.IsNil)

	reader, err := relay.NewReader(relayDir, 1)
	c.Assert(err, check.IsNil)

	err = feedByRelayLog(reader, ld, cp)
	c.Assert(err, check.IsNil)

	ts := cp.TS()
	c.Assert(ts, check.Equals, int64(90) /* latest commit ts */)
	c.Assert(cp.Status(), check.Equals, checkpoint.StatusNormal)
}
