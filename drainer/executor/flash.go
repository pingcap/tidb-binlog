package executor

import (
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	"strings"
	"github.com/pingcap/tidb-binlog/pkg/dml"
)

// flashRowBatch is an in-memory row batch caching rows about to passed to flash.
// It's not thread-safe, so callers must take care of the synchronizing.
type flashRowBatch struct {
	sqlHead string
	columnSize int
	sizeLimit int
	rows []interface{}
}

func newFlashRowBatch(sql string, sizeLimit int) *flashRowBatch {
	pos := strings.LastIndex(sql, "(")
	sqlHead := sql[0:pos]
	values := sql[pos:]
	columnSize := strings.Count(values, "?")
	return &flashRowBatch{
		sqlHead:           sqlHead,
		columnSize: columnSize,
		sizeLimit: sizeLimit,
		rows: make([]interface{}, 0, (sizeLimit + 1024) * columnSize), // Loosing the space to tolerant a little more rows being added.
	}
}

// AddRow adds some rows into this row batch.
func (batch *flashRowBatch) AddRow(args []interface{}) error {
	if len(args) != batch.columnSize {
		return errors.Errorf("Row %v column size %d mismatches the row batch column size %d", args, len(args), batch.columnSize)
	}
	batch.rows = append(batch.rows, args...)
	log.Debug(fmt.Sprintf("Added row %v.", args))
	return nil
}

// Size returns the number of rows stored in this batch.
func (batch *flashRowBatch) Size() int {
	return len(batch.rows) / batch.columnSize
}

func (batch *flashRowBatch) Flush(db *sql.DB) error {
	// TODO: could use columnar write to boost performance, see columnar.go in clickhouse driver example.
	log.Debug(fmt.Sprintf("Flushing %d rows for \"%s\".", batch.Size(), batch.sqlHead))
	if batch.Size() == 0 {
		return nil
	}
	sql := batch.genSql()
	err := pkgsql.ExecuteSQLs(db, []string{sql}, [][]interface{}{batch.rows}, false)
	if err != nil {
		return err
	}
	batch.rows = make([]interface{}, 0, (batch.sizeLimit + 1024) * batch.columnSize)
	return nil
}

func (batch *flashRowBatch) genSql() string {
	placeHolders := fmt.Sprintf("(%s)", dml.GenColumnPlaceholders(batch.columnSize))
	values := make([]string, batch.Size())
	for i := range values {
		values[i] = placeHolders
	}
	valuesStr := strings.Join(values, ",")
	return fmt.Sprintf("%s %s;", batch.sqlHead, valuesStr)
}

type flashExecutor struct {
	sync.Mutex
	close chan bool

	timeLimit time.Duration
	sizeLimit int

	dbs        []*sql.DB
	rowBatches map[string][]*flashRowBatch

	err error
}

func newFlash(cfg *DBConfig) (Executor, error) {
	timeLimit, err := time.ParseDuration(cfg.TimeLimit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// TODO: check time limit validity, and give a default value (and a warning) if invalid.

	sizeLimit, err := strconv.Atoi(cfg.SizeLimit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// TODO: check size limit validity, and give a default value (and a warning) if invalid.

	hostAndPorts, err := pkgsql.ParseCHAddr(cfg.Host)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbs := make([]*sql.DB, 0, len(hostAndPorts))
	for _, hostAndPort := range hostAndPorts {
		db, err := pkgsql.OpenCH("clickhouse", hostAndPort.Host, hostAndPort.Port, cfg.User, cfg.Password)
		dbs = append(dbs, db)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	e := flashExecutor{
		close: make(chan bool),
		timeLimit:  timeLimit,
		sizeLimit:  sizeLimit,
		dbs:        dbs,
		rowBatches: make(map[string][]*flashRowBatch),
	}

	go e.flushRoutine()

	return &e, nil
}

func (e *flashExecutor) Execute(sqls []string, args [][]interface{}, commitTSs []int64, isDDL bool) error {
	e.Lock()
	defer e.Unlock()

	if e.err != nil {
		log.Error("Executor seeing error %v from the flush thread, exiting.", e.err)
		return errors.Trace(e.err)
	}

	if isDDL {
		// Flush all row batches.
		e.flushAll()
		if e.err != nil {
			log.Error("Executor seeing error %v when flushing, exiting.", e.err)
			return errors.Trace(e.err)
		}
		for _, db := range e.dbs {
			e.err = pkgsql.ExecuteSQLs(db, sqls, args, isDDL)
			if e.err != nil {
				return errors.Trace(e.err)
			}
		}
	} else {
		for i, row := range args {
			hashKey := e.partition(row[0].(int64))
			sql := sqls[i]
			args := row[1:]
			if _, ok := e.rowBatches[sql]; !ok {
				e.rowBatches[sql] = make([]*flashRowBatch, len(e.dbs), len(e.dbs))
			}
			if e.rowBatches[sql][hashKey] == nil {
				e.rowBatches[sql][hashKey] = newFlashRowBatch(sql, e.sizeLimit)
			}
			rb := e.rowBatches[sql][hashKey]
			e.err = rb.AddRow(args)
			if e.err != nil {
				return errors.Trace(e.err)
			}

			// Check if size limit exceeded.
			if rb.Size() >= e.sizeLimit {
				e.err = rb.Flush(e.dbs[hashKey])
				if e.err != nil {
					return errors.Trace(e.err)
				}
			}
		}
	}

	return nil
}

func (e *flashExecutor) Close() error {
	// Could have had error in async flush goroutine, log it.
	e.Lock()
	if e.err != nil {
		log.Error(e.err)
	}
	e.Unlock()

	// Wait for async flush goroutine to exit.
	log.Info("Waiting for flush thread to close.")
	e.close <- true

	hasError := false
	for _, db := range e.dbs {
		err := db.Close()
		if err != nil {
			hasError = true
			log.Error(err)
		}
	}
	if hasError {
		return errors.New("error in closing some flash connector, check log for details")
	}
	return nil
}

func (e *flashExecutor) flushRoutine() {
	log.Info("Flush thread started.")
	for {
		time.Sleep(e.timeLimit)
		select {
		case <- e.close:
			log.Info("Flush thread closing.")
			return
		default:
			e.Lock()
			log.Debug("Flush thread reached time limit, flushing.")
			if e.err != nil {
				e.Unlock()
				log.Error("Flush thread seeing error %v from the executor, exiting.", e.err)
				return
			}
			e.flushAll()
			if e.err != nil {
				e.Unlock()
				log.Error("Flush thread seeing error %v when flushing, exiting.", e.err)
				return
			}
			// TODO: save checkpoint.
			e.Unlock()
		}
	}
}

// partition must be a index of dbs
func (e *flashExecutor) partition(key int64) int {
	return int(key % int64(len(e.dbs)))
}

func (e *flashExecutor) flushAll() {
	log.Debug(fmt.Sprintf("Flushing all row batches."))
	for _, rbs := range e.rowBatches {
		for i, rb := range rbs {
			e.err = rb.Flush(e.dbs[i])
			if e.err != nil {
				return
			}
		}
	}
}
