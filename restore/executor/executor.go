package executor

import (
	"fmt"
)

// execute sql to target database.

type Executor interface {
	// Execute executes sqls into target database.
	Execute(sqls []string, args [][]interface{}, isDDL bool) error
	// Close closes the Executors
	Close() error
}

func New(name string, cfg *DBConfig) (Executor, error) {
	switch name {
	case "mysql", "tidb":
		return newMysqlExecutor(cfg)
	case "print":
		return newDummyExecutor()
	}
	panic(fmt.Sprintf("unknown executor %s", name))
}
