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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/store/tikv/oracle"
	gb "github.com/pingcap/tipb/go-binlog"
)

var _ = Suite(&testFileSuite{})

type testFileSuite struct{}

func (s *testFileSuite) TestIsAcceptableBinlogFile(c *C) {
	binlogDir := c.MkDir()
	baseTS := int64(oracle.ComposeTS(time.Now().Unix()*1000, 0))

	// set SegmentSizeBytes to 1 can rotate binlog file after every binlog write
	segmentSizeBytes := binlogfile.SegmentSizeBytes
	binlogfile.SegmentSizeBytes = 1
	defer func() {
		binlogfile.SegmentSizeBytes = segmentSizeBytes
	}()

	// create binlog file
	for i := 0; i < 10; i++ {
		binlog := &pb.Binlog{
			CommitTs: baseTS + int64(i),
		}
		binlogData, err := binlog.Marshal()
		c.Assert(err, IsNil)

		binloger, err := binlogfile.OpenBinlogger(binlogDir, compress.CompressionNone)
		c.Assert(err, IsNil)
		binloger.WriteTail(&gb.Entity{Payload: binlogData})
		err = binloger.Close()
		c.Assert(err, IsNil)
	}

	reparos := []*Reparo{
		{
			cfg: &Config{
				Dir:      binlogDir,
				StartTSO: baseTS,
				StopTSO:  baseTS + 9,
			},
		},
		{
			cfg: &Config{
				Dir:      binlogDir,
				StartTSO: baseTS + 1,
				StopTSO:  baseTS + 2,
			},
		},
		{
			cfg: &Config{
				Dir:      binlogDir,
				StartTSO: baseTS + 2,
			},
		},
		{
			cfg: &Config{
				Dir:     binlogDir,
				StopTSO: baseTS + 2,
			},
		},
	}
	expectFileNums := []int{10, 2, 8, 3}

	allFiles, err := searchFiles(binlogDir)
	c.Assert(err, IsNil)

	for i, r := range reparos {
		files, err := filterFiles(allFiles, r.cfg.StartTSO, r.cfg.StopTSO)
		c.Assert(err, IsNil)
		c.Assert(files, HasLen, expectFileNums[i])
	}
}
