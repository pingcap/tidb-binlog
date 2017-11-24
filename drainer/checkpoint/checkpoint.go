package checkpoint

import (
	"github.com/juju/errors"
	pb "github.com/pingcap/tipb/go-binlog"
)

// CheckPoint is the binlog sync pos meta.
// When syncer restarts, we should reload meta info to guarantee continuous transmission.
type CheckPoint interface {
	// Load loads checkpoint information.
	Load() error

	// Save saves checkpoint information.
	Save(int64, map[string]pb.Pos) error

	// Pos gets position information.
	Pos() (int64, map[string]pb.Pos)

	// Check checks whether we should save checkpoint.
	Check() bool
}

// NewCheckPoint returns a CheckPoint instance by giving name
func NewCheckPoint(name string, cfg *Config) (CheckPoint, error) {
	switch name {
	case "mysql":
		return newMysql(cfg)
	case "pb":
		return newPb(cfg)
	default:
		return nil, errors.Errorf("unsupport SaveCheckPoint type %s", name)
	}
}
