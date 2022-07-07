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
	"database/sql"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

var _ Syncer = &MysqlSyncer{}

// QueueSizeGauge to be used.
var QueueSizeGauge *prometheus.GaugeVec

// MysqlSyncer sync binlog to Mysql
type MysqlSyncer struct {
	db      *sql.DB
	loader  loader.Loader
	relayer relay.Relayer
	*baseSyncer
}

// should only be used for unit test to create mock db
var createDB = loader.CreateDBWithSQLMode

// CreateLoader create the Loader instance.
func CreateLoader(
	db *sql.DB,
	cfg *DBConfig,
	worker int,
	batchSize int,
	queryHistogramVec *prometheus.HistogramVec,
	sqlMode *string,
	destDBType string,
	info *loopbacksync.LoopBackSync,
	enableDispatch bool,
	enableCausility bool,
) (ld loader.Loader, err error) {

	var opts []loader.Option
	opts = append(opts, loader.DestinationDBType(destDBType), loader.WorkerCount(worker), loader.BatchSize(batchSize),
		loader.SaveAppliedTS(destDBType == "tidb" || destDBType == "oracle"), loader.SetloopBackSyncInfo(info))
	if queryHistogramVec != nil {
		opts = append(opts, loader.Metrics(&loader.MetricsGroup{
			QueryHistogramVec: queryHistogramVec,
			EventCounterVec:   nil,
			QueueSizeGauge:    QueueSizeGauge,
		}))
	}

	opts = append(opts, loader.EnableDispatch(enableDispatch))
	opts = append(opts, loader.EnableCausality(enableCausility))
	opts = append(opts, loader.Merge(cfg.Merge))

	if cfg.SyncMode != 0 {
		mode := loader.SyncMode(cfg.SyncMode)
		opts = append(opts, loader.SyncModeOption(mode))
	}

	ld, err = loader.NewLoader(db, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return
}

// NewMysqlSyncer returns a instance of MysqlSyncer
func NewMysqlSyncer(
	cfg *DBConfig,
	tableInfoGetter translator.TableInfoGetter,
	worker int,
	batchSize int,
	queryHistogramVec *prometheus.HistogramVec,
	sqlMode *string,
	destDBType string,
	relayer relay.Relayer,
	info *loopbacksync.LoopBackSync,
	enableDispatch bool,
	enableCausility bool,
) (*MysqlSyncer, error) {
	if cfg.TLS != nil {
		log.Info("enable TLS to connect downstream MySQL/TiDB")
	}

	db, err := createDB(cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.TLS, sqlMode, cfg.Params, cfg.ReadTimeout)
	if err != nil {
		return nil, errors.Trace(err)
	}

	syncMode := loader.SyncMode(cfg.SyncMode)
	if syncMode == loader.SyncPartialColumn {
		var oldMode, newMode string
		oldMode, newMode, err = relaxSQLMode(db)
		if err != nil {
			db.Close()
			return nil, errors.Trace(err)
		}

		if newMode != oldMode {
			db.Close()
			db, err = createDB(cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.TLS, &newMode, cfg.Params, cfg.ReadTimeout)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	loader, err := CreateLoader(db, cfg, worker, batchSize, queryHistogramVec, sqlMode, destDBType, info, enableDispatch, enableCausility)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tzStr := ""
	if len(cfg.Params) > 0 {
		tzStr = cfg.Params["time_zone"]
	}
	timeZone, err := str2TimezoneOrFromDB(tzStr, db)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s := &MysqlSyncer{
		db:         db,
		loader:     loader,
		relayer:    relayer,
		baseSyncer: newBaseSyncer(tableInfoGetter, timeZone),
	}

	go s.run()

	return s, nil
}

// set newMode as the oldMode query from db by removing "STRICT_TRANS_TABLES".
func relaxSQLMode(db *sql.DB) (oldMode string, newMode string, err error) {
	row := db.QueryRow("SELECT @@SESSION.sql_mode;")
	err = row.Scan(&oldMode)
	if err != nil {
		return "", "", errors.Trace(err)
	}

	toRemove := "STRICT_TRANS_TABLES"
	newMode = oldMode

	if !strings.Contains(oldMode, toRemove) {
		return
	}

	// concatenated by "," like: mode1,mode2
	newMode = strings.Replace(newMode, toRemove+",", "", -1)
	newMode = strings.Replace(newMode, ","+toRemove, "", -1)
	newMode = strings.Replace(newMode, toRemove, "", -1)

	return
}

func str2TimezoneOrFromDB(tzStr string, db *sql.DB) (*time.Location, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var err error
	if len(tzStr) == 0 {
		dur, err := dbutil.GetTimeZoneOffset(ctx, db)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tzStr = dbutil.FormatTimeZoneOffset(dur)
	}
	if tzStr == "SYSTEM" || tzStr == "Local" {
		return nil, errors.New("'SYSTEM' or 'Local' time_zone is not supported")
	}

	loc, err := time.LoadLocation(tzStr)
	if err == nil {
		return loc, nil
	}

	// The value can be given as a string indicating an offset from UTC, such as '+10:00' or '-6:00'.
	// The time zone's value should in [-12:59,+14:00].
	// See: https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html#time-zone-variables
	if strings.HasPrefix(tzStr, "+") || strings.HasPrefix(tzStr, "-") {
		d, _, err := types.ParseDuration(nil, tzStr[1:], 0)
		if err == nil {
			if tzStr[0] == '-' {
				if d.Duration > 12*time.Hour+59*time.Minute {
					return nil, errors.Errorf("invalid timezone %s", tzStr)
				}
			} else {
				if d.Duration > 14*time.Hour {
					return nil, errors.Errorf("invalid timezone %s", tzStr)
				}
			}

			ofst := int(d.Duration / time.Second)
			if tzStr[0] == '-' {
				ofst = -ofst
			}
			name := dbutil.FormatTimeZoneOffset(d.Duration)
			return time.FixedZone(name, ofst), nil
		}
	}
	if err != nil {
		return nil, err
	}
	log.Info("use timezone", zap.String("location", loc.String()))
	return loc, nil
}

// SetSafeMode make the MysqlSyncer to use safe mode or not
func (m *MysqlSyncer) SetSafeMode(mode bool) bool {
	m.loader.SetSafeMode(mode)
	return true
}

// Sync implements Syncer interface
func (m *MysqlSyncer) Sync(item *Item) error {
	// `relayer` is nil if relay log is disabled.
	if m.relayer != nil {
		pos, err := m.relayer.WriteBinlog(item.Schema, item.Table, item.Binlog, item.PrewriteValue)
		if err != nil {
			return err
		}
		item.RelayLogPos = pos
	}

	txn, err := translator.TiBinlogToTxn(m.tableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue, item.ShouldSkip, m.timeZone)
	if err != nil {
		return errors.Trace(err)
	}
	txn.Metadata = item

	select {
	case <-m.errCh:
		return m.err
	case m.loader.Input() <- txn:
		return nil
	}
}

// Close implements Syncer interface
func (m *MysqlSyncer) Close() error {
	m.loader.Close()

	err := <-m.Error()

	if m.relayer != nil {
		closeRelayerErr := m.relayer.Close()
		if err == nil {
			err = closeRelayerErr
		}
	}

	return err
}

func (m *MysqlSyncer) run() {
	var wg sync.WaitGroup

	// handle success
	wg.Add(1)
	go func() {
		defer wg.Done()

		for txn := range m.loader.Successes() {
			item := txn.Metadata.(*Item)
			item.AppliedTS = txn.AppliedTS
			if m.relayer != nil {
				m.relayer.GCBinlog(item.RelayLogPos)
			}
			m.success <- item
		}
		close(m.success)
		log.Info("Successes chan quit")
	}()

	// run loader
	err := m.loader.Run()

	wg.Wait()
	m.db.Close()
	m.setErr(err)
}
