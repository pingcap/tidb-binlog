package loader

import "github.com/pingcap/tidb-binlog/drainer/loopbacksync"

type LoopBack interface {
	UpdateMarkTable(tx *Tx, info *loopbacksync.LoopBackSync) *Tx
}
