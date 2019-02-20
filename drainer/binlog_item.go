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

func newBinlogItem(b *pb.Binlog, nodeID string) *binlogItem {
	itemp := &binlogItem{
		binlog: b,
		nodeID: nodeID,
	}

	return itemp
}

//
func (b *binlogItem) SetJob(job *model.Job) {
	b.job = job
}
