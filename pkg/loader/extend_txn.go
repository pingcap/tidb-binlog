package loader

import (
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
)

type LoopBack interface {
	ExtendTxn(tx *Tx, dmls []*DML, info *loopbacksync.LoopBackSync) (*Tx, []*DML)
}
