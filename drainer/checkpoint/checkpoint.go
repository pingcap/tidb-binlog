package checkpoint

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
)

// CheckPoint is the binlog sync pos meta.
// When syncer restarts, we should reload meta info to guarantee continuous transmission.
type CheckPoint interface {
	// Load loads checkpoint information.
	Load() error

	// Save saves checkpoint information.
	Save(int64, map[string]pb.Pos) error

	// Check checks whether we should save checkpoint.
	Check() bool

	// Pos gets position information.
	Pos() (int64, map[string]pb.Pos)

	// String returns CommitTS and Offset
	String() string
}

// NewCheckPoint returns a CheckPoint instance by giving name
func NewCheckPoint(name string, cfg *Config) (CheckPoint, error) {
	var (
		cp  CheckPoint
		err error
	)
	switch name {
	case "mysql", "tidb":
		cp, err = newMysql(cfg)
	case "pb":
		cp, err = newPb(cfg)
	default:
		err = errors.Errorf("unsupport checkpoint type %s", name)
	}
	if err != nil {
		return nil, errors.Annotatef(err, "initial %s type checkpoint with config %+v", name, cfg)
	}

	log.Infof("initial %s type checkpoint %s with config %+v", name, cp, cfg)
	return cp, nil
}
