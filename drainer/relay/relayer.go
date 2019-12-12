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
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	tb "github.com/pingcap/tipb/go-binlog"
)

var _ Relayer = &relayer{}

// Relayer is the interface for writing relay log.
type Relayer interface {
	// WriteBinlog writes binlog to relay log file.
	WriteBinlog(schema string, table string, tiBinlog *tb.Binlog, pv *tb.PrewriteValue) (tb.Pos, error)

	// GCBinlog removes unused relay log files.
	GCBinlog(pos tb.Pos)

	// Close releases resources.
	Close() error
}

type relayer struct {
	tableInfoGetter translator.TableInfoGetter
	binlogger binlogfile.Binlogger
	// lastFileSuffix is the file suffix of last written relay log file.
	lastFileSuffix uint64
}

// NewRelayer creates a relayer.
func NewRelayer(dir string, maxFileSize int64, tableInfoGetter translator.TableInfoGetter) (Relayer, error) {
	// If `dir` is empty, it means relay log is disabled.
	if len(dir) == 0 {
		return nil, nil
	}

	binlogger, err := binlogfile.OpenBinlogger(dir, maxFileSize)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &relayer{
		tableInfoGetter: tableInfoGetter,
		binlogger: binlogger,
	}, nil
}

// WriteBinlog writes binlog to relay log.
func (r *relayer) WriteBinlog(schema string, table string, tiBinlog *tb.Binlog, pv *tb.PrewriteValue) (tb.Pos, error) {
	pos := tb.Pos{}
	binlog, err := translator.TiBinlogToSlaveBinlog(r.tableInfoGetter, schema, table, tiBinlog, pv)
	if err != nil {
		return pos, errors.Trace(err)
	}

	data, err := binlog.Marshal()
	if err != nil {
		return pos, errors.Trace(err)
	}

	pos, err = r.binlogger.WriteTail(&tb.Entity{Payload: data})
	if err != nil {
		return pos, errors.Trace(err)
	}

	return pos, nil
}

// GCBinlog removes unused relay log file.
func (r *relayer) GCBinlog(pos tb.Pos) {
	// If the file suffix increases, it means previous files are useless.
	if pos.Suffix > r.lastFileSuffix {
		r.binlogger.GC(0, pos)
		r.lastFileSuffix = pos.Suffix
	}
}

// Close closes binlogger.
func (r *relayer) Close() error {
	err := r.binlogger.Close()
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}