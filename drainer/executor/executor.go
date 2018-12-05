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
	// if recv err means fail to Execute something async, must quit ASAP
	Error() <-chan error
}

// New returns the an Executor instance by given name
func New(name string, cfg *DBConfig) (Executor, error) {
	switch name {
	case "mysql", "tidb":
		return newMysql(cfg)
	case "pb":
		return newPB(cfg)
	case "flash":
		return newFlash(cfg)
	case "kafka":
		return newKafka(cfg)
	default:
		return nil, errors.Errorf("unsupport executor type %s", name)
	}
}

func newBaseError() *baseError {
	return &baseError{
		errCh: make(chan struct{}),
	}
}

type baseError struct {
	err   error
	errCh chan struct{}
}

func (b *baseError) Error() <-chan error {
	ret := make(chan error, 1)
	go func() {
		<-b.errCh
		ret <- b.err
	}()

	return ret
}

func (b *baseError) SetErr(err error) {
	b.err = err
	close(b.errCh)
}
