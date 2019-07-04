package syncer

import (
	"fmt"

	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

// Syncer is the interface for executing binlog event to the target.
type Syncer interface {
	// Sync the binlog into target database.
	Sync(pbBinlog *pb.Binlog, successCB func(binlog *pb.Binlog)) error

	// Close closes the Syncer
	Close() error
}

// New creates a new executor based on the name.
func New(name string, cfg *DBConfig, safemode bool) (Syncer, error) {
	switch name {
	case "mysql":
		return newMysqlSyncer(cfg, safemode)
	case "print":
		return newPrintSyncer()
	case "memory":
		return newMemSyncer()
	}
	panic(fmt.Sprintf("unknown syncer %s", name))
}
