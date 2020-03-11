package loader

import (
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
)

// ExecutorExtend is the interface that for loader-plugin
type ExecutorExtend interface {
	ExtendTxn(tx *Tx, dmls []*DML, info *loopbacksync.LoopBackSync) (*Tx, []*DML)
}

// LoaderInit is the interface that for syncer-plugin
type LoaderInit interface {
	LoaderInit(s *loaderImpl) error
}

// LoaderInit is the interface that for syncer-plugin
type LoaderDestroy interface {
	LoaderDestroy(s *loaderImpl) error
}
