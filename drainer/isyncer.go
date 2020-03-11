package drainer

import (
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

// SyncerFilter is the interface that for syncer-plugin
type SyncerFilter interface {
	FilterTxn(txn *loader.Txn, info *loopbacksync.LoopBackSync) (bool, error)
}
