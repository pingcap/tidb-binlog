package syncer

import (
	"fmt"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/loader"
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
func New(name string, cfg *DBConfig) (Syncer, error) {
	switch name {
	case "mysql":
		db, err := loader.CreateDB(cfg.User, cfg.Password, cfg.Host, cfg.Port)
		if err != nil {
			log.Infof("create db failed %v", err)
			return nil, errors.Trace(err)
		}
		return newMysqlSyncer(db)
	case "print":
		return newPrintSyncer()
	case "memory":
		return newMemSyncer()
	}
	panic(fmt.Sprintf("unknown syncer %s", name))
}
