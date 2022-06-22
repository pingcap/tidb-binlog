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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	obinlog "github.com/pingcap/tidb/tidb-binlog/proto/go-binlog"
	"github.com/pingcap/tipb/go-binlog"
)

var _ Reader = &reader{}

// Reader is the interface for reading relay log.
type Reader interface {
	// Run reads relay log.
	Run() context.CancelFunc

	//  Binlogs returns the channel for reading parsed binlogs.
	Binlogs() <-chan *obinlog.Binlog

	// Close releases resources.
	Close() error

	// Error returns error occurs in reading.
	Error() <-chan error
}

type reader struct {
	binlogger binlogfile.Binlogger
	binlogs   chan *obinlog.Binlog
	err       chan error
}

// NewReader creates a relay reader.
func NewReader(dir string, readBufferSize int) (Reader, error) {
	binlogger, err := binlogfile.OpenBinlogger(dir, binlogfile.SegmentSizeBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &reader{
		binlogger: binlogger,
		binlogs:   make(chan *obinlog.Binlog, readBufferSize),
	}, nil
}

// Run implements Reader interface.
func (r *reader) Run() context.CancelFunc {
	r.err = make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	binlogChan, binlogErr := r.binlogger.ReadAll(ctx)

	go func(ctx context.Context) {
		var err error
		for err == nil {
			var blg *binlog.Entity
			select {
			case <-ctx.Done():
				err = ctx.Err()
				log.Warn("Reading relay log is interrupted")
			case blg = <-binlogChan:
			}
			if blg == nil {
				break
			}

			secondaryBinlog := new(obinlog.Binlog)
			if err = secondaryBinlog.Unmarshal(blg.Payload); err != nil {
				break
			}

			select {
			case <-ctx.Done():
				err = ctx.Err()
				log.Warn("Producing transaction is interrupted")
			case r.binlogs <- secondaryBinlog:
			}
		}
		// If binlogger is not done, notify it to stop.
		cancel()
		close(r.binlogs)

		if err == nil {
			err = <-binlogErr
		}
		if err != nil {
			r.err <- err
		}
		close(r.err)
	}(ctx)

	return cancel
}

// Txns implements Reader interface.
func (r *reader) Binlogs() <-chan *obinlog.Binlog {
	return r.binlogs
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
	if closeBinloggerErr := r.binlogger.Close(); err == nil {
		err = closeBinloggerErr
	}
	return errors.Trace(err)
}
