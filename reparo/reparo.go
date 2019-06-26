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

package reparo

import (
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/filter"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb-binlog/reparo/syncer"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
)

// Reparo i the main part of the recovery tool.
type Reparo struct {
	cfg    *Config
	syncer syncer.Syncer

	filter *filter.Filter
}

// New creates a Reparo object.
func New(cfg *Config) (*Reparo, error) {
	log.Info("New Reparo", zap.Stringer("config", cfg))

	syncer, err := syncer.New(cfg.DestType, cfg.DestDB, cfg.SafeMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	filter := filter.NewFilter(cfg.IgnoreDBs, cfg.IgnoreTables, cfg.DoDBs, cfg.DoTables)

	return &Reparo{
		cfg:    cfg,
		syncer: syncer,
		filter: filter,
	}, nil
}

// Process runs the main procedure.
func (r *Reparo) Process() error {
	pbReader, err := newDirPbReader(r.cfg.Dir, r.cfg.StartTSO, r.cfg.StopTSO)
	if err != nil {
		return errors.Annotatef(err, "new reader failed dir: %s", r.cfg.Dir)
	}
	defer pbReader.close()

	for {
		binlog, err := pbReader.read()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return nil
			}

			return errors.Trace(err)
		}

		ignore, err := filterBinlog(r.filter, binlog)
		if err != nil {
			return errors.Annotate(err, "filter binlog failed")
		}

		if ignore {
			continue
		}

		err = r.syncer.Sync(binlog, func(binlog *pb.Binlog) {
			dt := oracle.GetTimeFromTS(uint64(binlog.CommitTs))
			log.Info("sync binlog success", zap.Int64("ts", binlog.CommitTs), zap.Time("datetime", dt))
		})

		if err != nil {
			return errors.Annotate(err, "sync failed")
		}
	}
}

// Close closes the Reparo object.
func (r *Reparo) Close() error {
	return errors.Trace(r.syncer.Close())
}

// may drop some DML event of binlog
// return true if the whole binlog should be ignored
func filterBinlog(afilter *filter.Filter, binlog *pb.Binlog) (ignore bool, err error) {
	switch binlog.Tp {
	case pb.BinlogType_DDL:
		var table filter.TableName
		_, table, err = parseDDL(string(binlog.GetDdlQuery()))
		if err != nil {
			return false, errors.Annotatef(err, "parse ddl: %s failed", string(binlog.GetDdlQuery()))
		}

		if afilter.SkipSchemaAndTable(table.Schema, table.Table) {
			return true, nil
		}

		return
	case pb.BinlogType_DML:
		var events []pb.Event
		for _, event := range binlog.DmlData.GetEvents() {
			if afilter.SkipSchemaAndTable(event.GetSchemaName(), event.GetTableName()) {
				continue
			}

			events = append(events, event)
		}

		binlog.DmlData.Events = events
		if len(events) == 0 {
			ignore = true
		}
		return
	default:
		return false, errors.Errorf("unknown type: %d", binlog.Tp)
	}
}

func isAcceptableBinlog(binlog *pb.Binlog, startTs, endTs int64) bool {
	return binlog.CommitTs >= startTs && (endTs == 0 || binlog.CommitTs <= endTs)
}
