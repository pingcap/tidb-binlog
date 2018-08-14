package drainer

import (
	"fmt"

	"github.com/pingcap/tidb/model"
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

func (b *binlogItem) String() string {
	return fmt.Sprintf("{commitTS: %d, node: %s}", b.binlog.CommitTs, b.nodeID)
}

func newBinlogItem(b *pb.Binlog, nodeID string) *binlogItem {
	itemp := &binlogItem{
		binlog: b,
		nodeID: nodeID,
	}

	return itemp
}

func (b *binlogItem) SetJob(job *model.Job) {
	b.job = job
}
