package executor

type dummyExecutor struct{}

func newDummyExecutor() (Executor, error) {
	return &dummyExecutor{}, nil
}

func (d *dummyExecutor) Execute(sqls []string, args [][]interface{}, isDDL bool) error {
	// do nothing
	return nil
}

func (d *dummyExecutor) Close() error {
	// do nothing
	return nil
}
