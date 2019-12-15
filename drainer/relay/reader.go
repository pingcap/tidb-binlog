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

package relay

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	obinlog "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
)

var _ Reader = &reader{}

// Reader is the interface for reading relay log.
type Reader interface {
	// Run reads relay log.
	Run()

	// Txns returns parsed transactions.
	Txns() chan *loader.Txn

	// Close releases resources.
	Close() error

	// Error returns error occurs in reading.
	Error() <-chan error
}

type reader struct {
	binlogger binlogfile.Binlogger
	txns      chan *loader.Txn
	err       chan error
}

// NewReader creates a relay reader.
func NewReader(dir string) (Reader, error) {
	binlogger, err := binlogfile.OpenBinlogger(dir, binlogfile.SegmentSizeBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &reader{
		binlogger: binlogger,
		txns:      make(chan *loader.Txn, 8),
	}, nil
}

// Run implements Reader interface.
func (r *reader) Run() {
	stop := make(chan struct{})
	r.err = make(chan error, 1)
	binlogChan, binlogErr := r.binlogger.ReadAll(stop)

	go func() {
		var err error
		for binlog := range binlogChan {
			slaveBinlog := new(obinlog.Binlog)
			if err = slaveBinlog.Unmarshal(binlog.Payload); err != nil {
				break
			}

			txn, err := loader.SlaveBinlogToTxn(slaveBinlog)
			if err != nil {
				break
			}
			r.txns <- txn
		}
		// If binlogger is not done, notify it to stop.
		close(stop)
		close(r.txns)

		if err == nil {
			err = <-binlogErr
		}
		if err != nil {
			r.err <- err
		}
		close(r.err)
	}()
}

// Txns implements Reader interface.
func (r *reader) Txns() chan *loader.Txn {
	return r.txns
}

// Error implements Reader interface.
func (r *reader) Error() <-chan error {
	return r.err
}

// Close implements Reader interface.
func (r *reader) Close() error {
	var err error
	// If it's reading, wait until it's finished.
	if r.err != nil {
		err = <-r.err
	}
	if closeBinloggerErr := r.binlogger.Close(); err != nil {
		err = closeBinloggerErr
	}
	return errors.Trace(err)
}
