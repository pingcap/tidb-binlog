// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"github.com/pingcap/errors"
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
func New(name string, cfg *DBConfig, sqlMode *string) (Executor, error) {
	switch name {
	case "mysql", "tidb":
		return newMysql(cfg, sqlMode)
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
