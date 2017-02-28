package executor

import (
	"github.com/juju/errors"
)

// Executor is the interface for execute TiDB binlog's sql
type Executor interface {
	// Execute executes TiDB binlogs
	Execute([]string, [][]interface{}, []int64, bool) error
	// Close closes the executor
	Close() error
}

// New returns the an Executor instance by given name
func New(name string, cfg *DBConfig) (Executor, error) {
	switch name {
	case "mysql":
		return newMysql(cfg)
	case "pb":
		return newPB(cfg)
	default:
		return nil, errors.Errorf("unsupport executor type %s", name)
	}
}
