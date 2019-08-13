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
	"fmt"

	"github.com/pingcap/parser/model"
	pb "github.com/pingcap/tipb/go-binlog"
)

type binlogItem struct {
	binlog *pb.Binlog
	nodeID string
	job    *model.Job
	size   int64
}

// GetCommitTs implements Item interface in merger.go
func (b *binlogItem) GetCommitTs() int64 {
	return b.binlog.CommitTs
}

// GetSourceID implements Item interface in merger.go
func (b *binlogItem) GetSourceID() string {
	return b.nodeID
}

// String returns the string of this binlogItem
func (b *binlogItem) String() string {
	return fmt.Sprintf("{startTS: %d, commitTS: %d, node: %s}", b.binlog.StartTs, b.binlog.CommitTs, b.nodeID)
}

func newBinlogItem(b *pb.Binlog, nodeID string, s int) *binlogItem {
	itemp := &binlogItem{
		binlog: b,
		nodeID: nodeID,
		size:   int64(s),
	}

	return itemp
}

//
func (b *binlogItem) SetJob(job *model.Job) {
	b.job = job
}
