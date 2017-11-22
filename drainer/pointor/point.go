
package pointor
import (
    "github.com/juju/errors"
    pb "github.com/pingcap/tipb/go-binlog"
)

type Point interface {
    Load() error
    Save(int64, map[string]pb.Pos) error
    Pos() (int64, map[string]pb.Pos) 
    Check() bool
}

func NewPoint(name string, cfg *DBConfig) (Point, error) {
    switch name {
    case "mysql":
        return newMysqlPoint(cfg)
    case "pb":
        return newPbPoint(cfg)
    default:
        return nil, errors.Errorf("unsupport SaveCheckPointor type %s", name)
    }
}
