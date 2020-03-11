package loader

import (
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
)

// ExecutorExtend is the interface for loader plugin
type ExecutorExtend interface {
	ExtendTxn(tx *Tx, dmls []*DML, info *loopbacksync.LoopBackSync) (*Tx, []*DML)
}

// Init is the interface for loader plugin
type Init interface {
	LoaderInit(s *loaderImpl) error
}

// Destroy is the interface that for loader-plugin
type Destroy interface {
	LoaderDestroy(s *loaderImpl) error
}
