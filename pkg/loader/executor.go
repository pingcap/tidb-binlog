package loader

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"golang.org/x/sync/errgroup"
)

var defaultBatchSize = 128

type executor struct {
	db        *gosql.DB
	batchSize int
}

func newExecutor(db *gosql.DB) *executor {
	exe := &executor{
		db:        db,
		batchSize: defaultBatchSize,
	}

	return exe
}

func (e *executor) withBatchSize(batchSize int) *executor {
	e.batchSize = batchSize
	return e
}

func groupByTable(dmls []*DML) (tables map[string][]*DML) {
	if len(dmls) == 0 {
		return nil
	}

	tables = make(map[string][]*DML)
	for _, dml := range dmls {
		table := quoteSchema(dml.Database, dml.Table)
		tableDMLs := tables[table]
		tableDMLs = append(tableDMLs, dml)
		tables[table] = tableDMLs
	}

	return
}

func (e *executor) execTableBatchRetry(dmls []*DML, retryNum int, backoff time.Duration) error {
	var err error
	for i := 0; i < retryNum; i++ {
		if i > 0 {
			time.Sleep(backoff)
		}

		err = e.execTableBatch(dmls)
		if err == nil {
			return nil
		}
	}
	return errors.Trace(err)
}

func (e *executor) bulkDelete(deletes []*DML) error {
	var sqls []string
	var argss []interface{}

	for _, dml := range deletes {
		sql, args := dml.sql()
		sqls = append(sqls, sql)
		argss = append(argss, args...)
	}
	tx, err := e.db.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = tx.Exec(strings.Join(sqls, ";"), argss...)
	if err != nil {
		log.Error("exec fail sql: %s, args: %v", strings.Join(sqls, ";"), argss)
		tx.Rollback()
		return errors.Trace(err)
	}

	err = tx.Commit()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// no use now
// use INSERT INTO ON DUPLICATE KEY UPDATE
func (e *executor) bulkMergeSQL(updates []*DML) error {
	info := updates[0].info
	dbName := updates[0].Database
	tableName := updates[0].Table

	builder := new(strings.Builder)
	var holders []string
	holder := fmt.Sprintf("(%s)", holderString(len(info.columns)))
	for i := 0; i < len(updates); i++ {
		holders = append(holders, holder)
	}

	// look like float type has bug fail to update
	// https://github.com/pingcap/tidb/issues/8934
	builder.WriteString(fmt.Sprintf("INSERT INTO %s(%s) VALUES %s ON DUPLICATE KEY UPDATE ", quoteSchema(dbName, tableName), buildColumnList(info.columns), strings.Join(holders, ",")))

	for i, name := range info.columns {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString(fmt.Sprintf("%s=VALUES(%s)", name, name))
	}

	var args []interface{}
	for _, insert := range updates {
		for _, name := range info.columns {
			v := insert.Values[name]
			args = append(args, v)
		}
	}

	tx, err := e.db.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = tx.Exec(builder.String(), args...)
	if err != nil {
		log.Errorf("exec fail sql: %s, args: %v", builder.String(), args)
		tx.Rollback()
		return errors.Trace(err)
	}
	err = tx.Commit()
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (e *executor) bulkReplace(inserts []*DML) error {
	if len(inserts) == 0 {
		return nil
	}

	info := inserts[0].info
	dbName := inserts[0].Database
	tableName := inserts[0].Table

	builder := new(strings.Builder)
	builder.WriteString(fmt.Sprintf("REPLACE INTO %s(%s) VALUES ", quoteSchema(dbName, tableName), buildColumnList(info.columns)))

	var holders []string
	holder := fmt.Sprintf("(%s)", holderString(len(info.columns)))
	for i := 0; i < len(inserts); i++ {
		holders = append(holders, holder)
	}
	builder.WriteString(strings.Join(holders, ","))

	var args []interface{}
	for _, insert := range inserts {
		for _, name := range info.columns {
			v := insert.Values[name]
			args = append(args, v)
		}
	}

	tx, err := e.db.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = tx.Exec(builder.String(), args...)
	if err != nil {
		log.Errorf("exec fail sql: %s, args: %v", builder.String(), args)
		tx.Rollback()
		return errors.Trace(err)
	}
	err = tx.Commit()
	if err != nil {
		return errors.Trace(err)
	}
	return nil

}

// we merge dmls by primary key, after merge by key, we
// have only one dml for one primary key which contains the newest value(like a kv store),
// to avoid other column's duplicate entry, we should apply delete dmls first, then insert&update
// use replace to handle the update unique index case(see https://github.com/pingcap/tidb-binlog/pull/437/files)
// or we can simply check if it update unique index column or not, and for update change to (delete + insert)
// the final result should has no duplicate entry or the origin dmls is wrong.
func (e *executor) execTableBatch(dmls []*DML) error {
	if len(dmls) == 0 {
		return nil
	}

	types, err := mergeByKey(dmls)
	if err != nil {
		return errors.Trace(err)
	}

	log.Debugf("dmls: %v after merge: %v", dmls, types)

	if allDeletes, ok := types[DeleteDMLType]; ok {
		errg, _ := errgroup.WithContext(context.Background())

		for _, deletes := range splitDMLs(allDeletes, e.batchSize) {

			deletes := deletes

			errg.Go(func() error {
				err := e.bulkDelete(deletes)
				return errors.Trace(err)
			})
		}

		err := errg.Wait()
		if err != nil {
			return errors.Trace(err)
		}
	}

	if allInserts, ok := types[InsertDMLType]; ok {
		errg, _ := errgroup.WithContext(context.Background())

		for _, inserts := range splitDMLs(allInserts, e.batchSize) {
			inserts := inserts
			errg.Go(func() error {
				err := e.bulkReplace(inserts)
				return errors.Trace(err)
			})
		}

		err := errg.Wait()
		if err != nil {
			return errors.Trace(err)
		}
	}

	if allUpdates, ok := types[UpdateDMLType]; ok {
		errg, _ := errgroup.WithContext(context.Background())

		for _, updates := range splitDMLs(allUpdates, e.batchSize) {

			updates := updates

			errg.Go(func() error {
				// err := e.bulkMergeSQL(updates)
				err := e.bulkReplace(updates)
				if err != nil {
					return errors.Trace(err)
				}
				return nil
			})
		}
		err := errg.Wait()
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (e *executor) singleExecRetry(allDMLs []*DML, safeMode bool, retryNum int, backoff time.Duration) error {
	var err error

	for _, dmls := range splitDMLs(allDMLs, e.batchSize) {
		var i int
		for i = 0; i < retryNum; i++ {
			if i > 0 {
				time.Sleep(backoff)
			}

			err = e.singleExec(dmls, safeMode)
			if err == nil {
				break
			}
		}
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (e *executor) singleExec(dmls []*DML, safeMode bool) error {
	tx, err := e.db.Begin()
	if err != nil {
		return errors.Trace(err)
	}

	for _, dml := range dmls {
		if safeMode && dml.Tp == UpdateDMLType {
			sql, args := dml.deleteSQL()
			log.Debugf("exec: %s, args: %v", sql, args)
			_, err := tx.Exec(sql, args...)
			if err != nil {
				log.Errorf("err: %v, exec dml sql: %s, args: %v", err, sql, args)
				tx.Rollback()
				return errors.Trace(err)
			}

			sql, args = dml.replaceSQL()
			log.Debugf("exec: %s, args: %v", sql, args)
			_, err = tx.Exec(sql, args...)
			if err != nil {
				log.Errorf("err: %v, exec dml sql: %s, args: %v", err, sql, args)
				tx.Rollback()
				return errors.Trace(err)
			}

		} else {
			sql, args := dml.sql()
			log.Debugf("exec dml sql: %s, args: %v", sql, args)
			_, err := tx.Exec(sql, args...)
			if err != nil {
				log.Errorf("err: %v, exec dml sql: %s, args: %v", err, sql, args)
				tx.Rollback()
				return errors.Trace(err)
			}
		}
	}

	err = tx.Commit()
	return errors.Trace(err)
}
