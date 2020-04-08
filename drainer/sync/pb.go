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

package sync

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	tb "github.com/pingcap/tipb/go-binlog"
)

var _ Syncer = &pbSyncer{}

type pbSyncer struct {
	*BaseSyncer

	binlogger binlogfile.Binlogger
	cancel    func()
}

// NewPBSyncer sync binlog to files
func NewPBSyncer(dir string, retentionDays int, tableInfoGetter translator.TableInfoGetter) (*pbSyncer, error) {
	binlogger, err := binlogfile.OpenBinlogger(dir, binlogfile.SegmentSizeBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx, cancel := context.WithCancel(context.TODO())

	s := &pbSyncer{
		binlogger:  binlogger,
		BaseSyncer: newBaseSyncer(tableInfoGetter),
		cancel:     cancel,
	}

	if retentionDays > 0 {
		// TODO: Add support for human readable format input of times like "7d", "12h"
		retentionTime := time.Duration(retentionDays) * 24 * time.Hour
		ticker := time.NewTicker(time.Hour)
		go func() {
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					log.Info("Binlog GC loop stopped")
					return
				case <-ticker.C:
					log.Info("Trying to GC binlog files")
					binlogger.GCByTime(retentionTime)
				}
			}
		}()
	}

	return s, nil
}

func (p *pbSyncer) Sync(item *Item) error {
	pbBinlog, err := translator.TiBinlogToPbBinlog(p.tableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue)
	if err != nil {
		return errors.Trace(err)
	}

	err = p.saveBinlog(pbBinlog)
	if err != nil {
		return errors.Trace(err)
	}

	p.success <- item

	return nil
}

func (p *pbSyncer) saveBinlog(binlog *pb.Binlog) error {
	data, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	_, err = p.binlogger.WriteTail(&tb.Entity{Payload: data})
	return errors.Trace(err)
}

func (p *pbSyncer) Close() error {
	p.cancel()

	err := p.binlogger.Close()
	p.setErr(err)
	close(p.success)

	return p.err
}
