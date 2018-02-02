package executor

// execute sql to target database.

// TODO

type Executor interface {
	// Execute executes sqls into target database.
	Execute(sqls []string, args [][]interface{}) error
	// Close closes the Executors
	Close() error
}
