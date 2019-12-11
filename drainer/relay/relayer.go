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
	"github.com/pingcap/tidb-binlog/drainer/sync"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	tb "github.com/pingcap/tipb/go-binlog"
)

var _ Relayer = &relayer{}

type Relayer interface {
	WriteBinlog(item *sync.Item) error
	GCBinlog(item *sync.Item)
	Recover() error
	Close() error
}

type relayer struct {
	tableInfoGetter translator.TableInfoGetter
	binlogger binlogfile.Binlogger
	lastFileSuffix uint64
}

func NewRelayer(dir string, maxFileSize int64, tableInfoGetter translator.TableInfoGetter) (Relayer, error) {
	binlogger, err := binlogfile.OpenBinlogger(dir, maxFileSize)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &relayer{
		tableInfoGetter: tableInfoGetter,
		binlogger: binlogger}, nil
}

func (r *relayer) WriteBinlog(item *sync.Item) error {
	binlog, err := translator.TiBinlogToSlaveBinlog(r.tableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue)
	if err != nil {
		return errors.Trace(err)
	}

	data, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	pos, err := r.binlogger.WriteTail(&tb.Entity{Payload: data})
	if err != nil {
		return errors.Trace(err)
	}

	item.RelayLogPos = pos
	return nil
}

func (r *relayer) GCBinlog(item *sync.Item) {
	pos := item.RelayLogPos
	if pos.Suffix > r.lastFileSuffix {
		r.binlogger.GC(0, pos)
		r.lastFileSuffix = pos.Suffix
	}
}

func (r *relayer) Recover() error {
	return nil
}

func (r *relayer) Close() error {
	err := r.binlogger.Close()
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}