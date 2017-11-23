package checkpoint

import (
	"github.com/juju/errors"
	pb "github.com/pingcap/tipb/go-binlog"
)

// SaveCheckPoint is the binlog sync pos meta.
// When syncer restarts, we should reload meta info to guarantee continuous transmission.
type SaveCheckPoint interface {
	// Load loads checkpoint information.
	Load() error

	// Save saves checkpoint information.
	Save(int64, map[string]pb.Pos) error

	// Pos gets position information.
	Pos() (int64, map[string]pb.Pos)

	// Check checks whether we should save checkpoint.
	Check() bool
}

// NewSaveCheckPoint return an instance by giving name
func NewSaveCheckPoint(name string, cfg *DBConfig) (SaveCheckPoint, error) {
	switch name {
	case "mysql":
		return newMysqlSavePoint(cfg)
	case "pb":
		return newPbSavePoint(cfg)
	default:
		return nil, errors.Errorf("unsupport SaveCheckPoint type %s", name)
	}
}
