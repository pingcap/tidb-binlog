package drainer

import (
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

// LoopBack is the interface that for syncer-plugin
type LoopBack interface {
	FilterTxn(txn *loader.Txn, info *loopbacksync.LoopBackSync) (bool, error)
}

type SyncerInit interface {
	SyncerInit(s *Syncer) error
}
