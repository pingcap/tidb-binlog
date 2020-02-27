package drainer

import (
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

type LoopBack interface {
	FilterTxn(txn *loader.Txn, info *loopbacksync.LoopBackSync) (bool, error)
}
