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

package loader

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/godror/godror"

	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb/infoschema"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	defaultBatchSize   = 128
	defaultWorkerCount = 16
	index              int64
)

type executor struct {
	db                  *gosql.DB
	destDBType          string
	batchSize           int
	workerCount         int
	info                *loopbacksync.LoopBackSync
	queryHistogramVec   *prometheus.HistogramVec
	refreshTableInfo    func(schema string, table string) (info *tableInfo, err error)
	fTryRefreshTableErr func(err error) bool
	fSingleExec         func(dmls []*DML, safeMode bool) error
}

func newExecutor(db *gosql.DB) *executor {
	exe := &executor{
		db:                  db,
		batchSize:           defaultBatchSize,
		workerCount:         defaultWorkerCount,
		fTryRefreshTableErr: tryRefreshTableErr,
	}
	//default using tidb singleExec
	exe.fSingleExec = exe.singleExec
	return exe
}

func (e *executor) withRefreshTableInfo(fn func(schema string, table string) (info *tableInfo, err error)) *executor {
	e.refreshTableInfo = fn
	return e
}

func (e *executor) withDestDBType(destDBType string) *executor {
	e.destDBType = destDBType
	return e
}

func (e *executor) withBatchSize(batchSize int) *executor {
	e.batchSize = batchSize
	return e
}

func (e *executor) setSyncInfo(info *loopbacksync.LoopBackSync) {
	e.info = info
}

func (e *executor) setWorkerCount(workerCount int) {
	e.workerCount = workerCount
}

func (e *executor) withQueryHistogramVec(queryHistogramVec *prometheus.HistogramVec) *executor {
	e.queryHistogramVec = queryHistogramVec
	return e
}

func (e *executor) execTableBatchRetry(ctx context.Context, dmls []*DML, retryNum int, backoff time.Duration) error {
	err := util.RetryContext(ctx, retryNum, backoff, 1, func(context.Context) error {
		return e.execTableBatch(ctx, dmls)
	})
	return errors.Trace(err)
}

// a wrap of *sql.Tx with metrics
type tx struct {
	*gosql.Tx
	queryHistogramVec *prometheus.HistogramVec
}

// wrap of sql.Tx.Exec()
func (tx *tx) exec(query string, args ...interface{}) (gosql.Result, error) {
	start := time.Now()
	var (
		res gosql.Result
		err error
	)
	if len(args) == 0 {
		res, err = tx.Tx.Exec(query)
	} else {
		res, err = tx.Tx.Exec(query, args...)
	}
	if tx.queryHistogramVec != nil {
		tx.queryHistogramVec.WithLabelValues("exec").Observe(time.Since(start).Seconds())
	}

	return res, err
}

func (tx *tx) autoRollbackExec(query string, args ...interface{}) (res gosql.Result, err error) {
	if len(args) == 0 {
		res, err = tx.exec(query)
	} else {
		res, err = tx.exec(query, args...)
	}
	if err != nil {
		log.Error("Exec fail, will rollback", zap.String("query", query), zap.Reflect("args", args), zap.Error(err))
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Auto rollback", zap.Error(rbErr))
		}
		err = errors.Trace(err)
	}
	return
}

// wrap of sql.Tx.Commit()
func (tx *tx) commit() error {
	start := time.Now()
	err := tx.Tx.Commit()
	if tx.queryHistogramVec != nil {
		tx.queryHistogramVec.WithLabelValues("commit").Observe(time.Since(start).Seconds())
	}

	return errors.Trace(err)
}

func (e *executor) addIndex() int64 {
	return atomic.AddInt64(&index, 1) % ((int64)(e.workerCount))
}

// return a wrap of sql.Tx
func (e *executor) begin() (*tx, error) {
	sqlTx, err := e.db.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}

	var tx = &tx{
		Tx:                sqlTx,
		queryHistogramVec: e.queryHistogramVec,
	}

	if e.info != nil && e.info.LoopbackControl {
		start := time.Now()

		err = loopbacksync.UpdateMark(tx.Tx, e.addIndex(), e.info.ChannelID)
		if err != nil {
			rerr := tx.Rollback()
			if rerr != nil {
				log.Error("fail to rollback", zap.Error(rerr))
			}
			return nil, errors.Annotate(err, "failed to update mark data")
		}

		if tx.queryHistogramVec != nil {
			tx.queryHistogramVec.WithLabelValues("update_mark_table").Observe(time.Since(start).Seconds())
		}
	}

	return tx, nil
}

func (e *executor) bulkDelete(deletes []*DML) error {
	if len(deletes) == 0 {
		return nil
	}

	var sqls strings.Builder
	argss := make([]interface{}, 0, len(deletes))

	for _, dml := range deletes {
		sql, args := dml.sql()
		sqls.WriteString(sql)
		sqls.WriteByte(';')
		argss = append(argss, args...)
	}
	tx, err := e.begin()
	if err != nil {
		return errors.Trace(err)
	}
	sql := sqls.String()
	_, err = tx.autoRollbackExec(sql, argss...)
	if err != nil {
		return errors.Trace(err)
	}

	err = tx.commit()
	return errors.Trace(err)
}

func (e *executor) bulkReplace(inserts []*DML) error {
	if len(inserts) == 0 {
		return nil
	}

	info := inserts[0].info

	var builder strings.Builder

	cols := "(" + buildColumnList(info.columns) + ")"
	builder.WriteString("REPLACE INTO " + inserts[0].TableName() + cols + " VALUES ")

	holder := fmt.Sprintf("(%s)", holderString(len(info.columns)))
	for i := 0; i < len(inserts); i++ {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(holder)
	}

	args := make([]interface{}, 0, len(inserts)*len(info.columns))
	for _, insert := range inserts {
		for _, name := range info.columns {
			v := insert.Values[name]
			args = append(args, v)
		}
	}
	tx, err := e.begin()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = tx.autoRollbackExec(builder.String(), args...)
	if err != nil {
		return errors.Trace(err)
	}
	err = tx.commit()
	return errors.Trace(err)
}

// TODO: add bulkOperation for oracle
func (e *executor) oracleBulkOperation(dmls []*DML) error {
	if len(dmls) == 0 {
		return nil
	}
	tx, err := e.begin()
	if err != nil {
		return errors.Trace(err)
	}
	for _, dml := range dmls {
		sql := dml.oracleSQL()
		_, err = tx.autoRollbackExec(sql)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = tx.commit()
	return errors.Trace(err)

}

// we merge dmls by primary key, after merge by key, we
// have only one dml for one primary key which contains the newest value(like a kv store),
// to avoid other column's duplicate entry, we should apply delete dmls first, then insert&update
// use replace to handle the update unique index case(see https://github.com/pingcap/tidb-binlog/pull/437/files)
// or we can simply check if it update unique index column or not, and for update change to (delete + insert)
// the final result should has no duplicate entry or the origin dmls is wrong.
func (e *executor) execTableBatch(ctx context.Context, dmls []*DML) error {
	if len(dmls) == 0 {
		return nil
	}

	types, err := mergeByPrimaryKey(dmls)
	if err != nil {
		return errors.Trace(err)
	}

	log.Debug("merge dmls", zap.Reflect("dmls", dmls), zap.Reflect("merged", types))

	if allDeletes, ok := types[DeleteDMLType]; ok {
		bulkDelete := e.bulkDelete
		if e.destDBType == "oracle" {
			bulkDelete = e.oracleBulkOperation
		}
		if err := e.splitExecDML(ctx, allDeletes, bulkDelete); err != nil {
			return errors.Trace(err)
		}
	}

	if allInserts, ok := types[InsertDMLType]; ok {
		bulkInsert := e.bulkReplace
		if e.destDBType == "oracle" {
			bulkInsert = e.oracleBulkOperation
		}
		if err := e.splitExecDML(ctx, allInserts, bulkInsert); err != nil {
			return errors.Trace(err)
		}
	}

	if allUpdates, ok := types[UpdateDMLType]; ok {
		bulkUpdate := e.bulkReplace
		if e.destDBType == "oracle" {
			bulkUpdate = e.oracleBulkOperation
		}
		if err := e.splitExecDML(ctx, allUpdates, bulkUpdate); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// splitExecDML split dmls to size of e.batchSize and call exec concurrently
func (e *executor) splitExecDML(ctx context.Context, dmls []*DML, exec func(dmls []*DML) error) error {
	errg, _ := errgroup.WithContext(ctx)

	for _, split := range splitDMLs(dmls, e.batchSize) {
		split := split
		errg.Go(func() error {
			err := exec(split)
			if err != nil {
				return errors.Trace(err)
			}
			return nil
		})
	}

	return errors.Trace(errg.Wait())
}

func tryRefreshTableErr(err error) bool {
	errCode, ok := pkgsql.GetSQLErrCode(err)
	if !ok {
		return false
	}

	switch errCode {
	case infoschema.ErrColumnNotExists.Code():
		return true
	}

	return false
}

func tryRefreshTableOracleErr(err error) bool {
	oraErr, ok := godror.AsOraErr(err)
	if !ok {
		return false
	}
	// Invalid identifier for oracle db error
	// https://docs.oracle.com/database/121/DRDAS/error_code.htm#DRDAS513
	if oraErr.Code() == 904 {
		return true
	}
	return false
}

func (e *executor) singleExecRetry(ctx context.Context, allDMLs []*DML, safeMode bool, retryNum int, backoff time.Duration) error {
	var execErr error
	for _, dmls := range splitDMLs(allDMLs, e.batchSize) {
		err := util.RetryContext(ctx, retryNum, backoff, 1, func(context.Context) error {
			execErr = e.fSingleExec(dmls, safeMode)
			if execErr == nil {
				return nil
			}

			if e.fTryRefreshTableErr(execErr) && e.refreshTableInfo != nil {
				log.Info("try refresh table info")
				name2info := make(map[string]*tableInfo)
				for _, dml := range dmls {
					name := dml.TableName()
					info, ok := name2info[name]
					if !ok {
						var err error
						info, err = e.refreshTableInfo(dml.Database, dml.Table)
						if err != nil {
							log.Error("fail to refresh table info", zap.Error(err))
							continue
						}

						name2info[name] = info
					}

					if len(dml.info.columns) != len(info.columns) {
						log.Info("columns change", zap.Strings("old", dml.info.columns),
							zap.Strings("new", info.columns))
						removeOrphanCols(info, dml)
					}
					dml.info = info
				}
			}
			return execErr
		})
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (e *executor) singleExec(dmls []*DML, safeMode bool) error {
	tx, err := e.begin()
	if err != nil {
		return errors.Trace(err)
	}

	for _, dml := range dmls {
		if safeMode && dml.Tp == UpdateDMLType {
			sql, args := dml.deleteSQL()
			_, err := tx.autoRollbackExec(sql, args...)
			if err != nil {
				return errors.Trace(err)
			}

			sql, args = dml.replaceSQL()
			_, err = tx.autoRollbackExec(sql, args...)
			if err != nil {
				return errors.Trace(err)
			}
		} else if safeMode && dml.Tp == InsertDMLType {
			sql, args := dml.replaceSQL()
			_, err := tx.autoRollbackExec(sql, args...)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			sql, args := dml.sql()
			_, err := tx.autoRollbackExec(sql, args...)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	err = tx.commit()
	return errors.Trace(err)
}

func (e *executor) singleOracleExec(dmls []*DML, safeMode bool) error {
	tx, err := e.begin()
	if err != nil {
		return errors.Trace(err)
	}

	for _, dml := range dmls {
		if safeMode && dml.Tp == UpdateDMLType {
			//delete old row
			sql := dml.oracleDeleteSQL()
			log.Debug("safeMode and UpdateDMLType", zap.String("delete old", sql))
			_, err := tx.autoRollbackExec(sql)
			if err != nil {
				return errors.Trace(err)
			}
			//delete new row
			sql = dml.oracleDeleteNewValueSQL()
			log.Debug("safeMode and UpdateDMLType", zap.String("delete new old", sql))
			_, err = tx.autoRollbackExec(sql)
			if err != nil {
				return errors.Trace(err)
			}
			//insert new row
			sql = dml.oracleInsertSQL()
			log.Debug("safeMode and UpdateDMLType", zap.String("insert new old", sql))
			_, err = tx.autoRollbackExec(sql)
			if err != nil {
				return errors.Trace(err)
			}
		} else if safeMode && dml.Tp == InsertDMLType {
			sql := dml.oracleDeleteSQL()
			log.Debug("safeMode and InsertDMLType", zap.String("delete sql", sql))
			_, err := tx.autoRollbackExec(sql)
			if err != nil {
				return errors.Trace(err)
			}
			sql = dml.oracleInsertSQL()
			log.Debug("safeMode and InsertDMLType", zap.String("insert sql", sql))
			_, err = tx.autoRollbackExec(sql)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			sql := dml.oracleSQL()
			log.Debug("normal sql with no safeMode", zap.String("sql", sql))
			_, err := tx.autoRollbackExec(sql)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	err = tx.commit()
	return errors.Trace(err)
}
