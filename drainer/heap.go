package drainer

import (
	"fmt"
	"time"

	"github.com/pingcap/tidb/model"
	pb "github.com/pingcap/tipb/go-binlog"
)

var (
	pushRetryTime = 10 * time.Millisecond
)

type binlogItem struct {
	binlog *pb.Binlog
	nodeID string
	job    *model.Job
}

// GetCommitTs inplement Item interface in merger.go
func (b *binlogItem) GetCommitTs() int64 {
	return b.binlog.CommitTs
}

func (b *binlogItem) String() string {
	return fmt.Sprintf("{commitTS: %d, node: %s}", b.binlog.CommitTs, b.nodeID)
}

func newBinlogItem(b *pb.Binlog, ts int64, nodeID string) *binlogItem {
	itemp := &binlogItem{
		binlog: b,
		nodeID: nodeID,
	}

	return itemp
}

func (b *binlogItem) SetJob(job *model.Job) {
	b.job = job
}
