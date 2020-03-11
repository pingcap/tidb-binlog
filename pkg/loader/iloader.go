package loader

import (
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
)

// LoopBack is the interface that for loader-plugin
type ExecutorExtend interface {
	ExtendTxn(tx *Tx, dmls []*DML, info *loopbacksync.LoopBackSync) (*Tx, []*DML)
}
