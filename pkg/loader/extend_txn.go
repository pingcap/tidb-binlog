package loader

import "github.com/pingcap/tidb-binlog/drainer/loopbacksync"

type LoopBack interface {
	ExtendTxn(tx *Tx, info *loopbacksync.LoopBackSync) *Tx
}
