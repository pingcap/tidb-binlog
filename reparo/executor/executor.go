package executor

import (
	"fmt"
)

// execute sql to target database.

// Executor is the interface for executing binlog event to the target.
type Executor interface {
	// Execute executes sqls into target database.
	Execute(sqls []string, args [][]interface{}, isDDL bool) error
	// Close closes the Executors
	Close() error
}

// New creates a new executor based on the name.
func New(name string, cfg *DBConfig) (Executor, error) {
	switch name {
	case "mysql":
		return newMysqlExecutor(cfg)
	case "print":
		return newDummyExecutor()
	}
	panic(fmt.Sprintf("unknown executor %s", name))
}
