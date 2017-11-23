package checkpoint
import (
    pb "github.com/pingcap/tipb/go-binlog"
    "github.com/juju/errors"
)
type SaveCheckPoint interface {
    Load() error
    Save(int64, map[string]pb.Pos) error
    Pos() (int64, map[string]pb.Pos) 
    Check() bool
}

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
