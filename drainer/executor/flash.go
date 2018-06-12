package executor

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/kshvakov/clickhouse"
	"github.com/ngaut/log"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
)

var extraRowSize = 1024

// flashRowBatch is an in-memory row batch caching rows about to passed to flash.
// It's not thread-safe, so callers must take care of the synchronizing.
type flashRowBatch struct {
	sql        string
	columnSize int
	capacity   int
	rows       [][]driver.Value
}

func newFlashRowBatch(sql string, capacity int) *flashRowBatch {
	pos := strings.LastIndex(sql, "(")
	values := sql[pos:]
	columnSize := strings.Count(values, "?")
	// Loosing the space to tolerant a little more rows being added.
	rows := make([][]driver.Value, 0, capacity+extraRowSize)
	return &flashRowBatch{
		sql:        sql,
		columnSize: columnSize,
		capacity:   capacity,
		rows:       rows,
	}
}

// AddRow appends single row into this row batch.
func (batch *flashRowBatch) AddRow(args []interface{}) error {
	if len(args) != batch.columnSize {
		return errors.Errorf("Row %v column size %d mismatches the row batch column size %d", args, len(args), batch.columnSize)
	}
	dargs := make([]driver.Value, 0, len(args))
	for _, c := range args {
		dargs = append(dargs, c)
	}
	batch.rows = append(batch.rows, dargs)
	log.Debug(fmt.Sprintf("Added row %v.", args))
	return nil
}

// Size returns the number of rows stored in this batch.
func (batch *flashRowBatch) Size() int {
	return len(batch.rows)
}

// Flush writes all the rows in this row batch into CH.
func (batch *flashRowBatch) Flush(conn clickhouse.Clickhouse) (err error) {
	log.Debug(fmt.Sprintf("Flushing %d rows for \"%s\".", batch.Size(), batch.sql))
	defer func() {
		if err != nil {
			log.Errorf("Flushing rows for \"%d\" failed due to error %v.", batch.sql, err)
		} else {
			log.Debug(fmt.Sprintf("Flushed %d rows for \"%s\".", batch.Size(), batch.sql))
		}
	}()

	if batch.Size() == 0 {
		return nil
	}

	tx, err := conn.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	stmt, err := conn.Prepare(batch.sql)
	if err != nil {
		return errors.Trace(err)
	}
	defer stmt.Close()
	block, err := conn.Block()
	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range batch.rows {
		err = block.AppendRow(row)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = conn.WriteBlock(block)
	if err != nil {
		return errors.Trace(err)
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		if ce, ok := err.(*clickhouse.Exception); ok {
			// Stack trace from server side could be very helpful for triaging problems.
			log.Error(ce.StackTrace)
		}
		return errors.Trace(err)
	}
	// Clearing all rows.
	// Loosing the space to tolerant a little more rows being added.
	batch.rows = make([][]driver.Value, 0, batch.capacity+extraRowSize)

	return nil
}

// chDB keeps two connection to CH:
// One through standard go SQL driver's DB, to leverage legacy pkgsql stuff.
// The other through raw CH SQL driver's connection, to use CH's block interface.
// A little bit ugly, but handy.
type chDB struct {
	DB   *sql.DB
	Conn clickhouse.Clickhouse
}

type flashExecutor struct {
	sync.Mutex
	close chan bool

	timeLimit time.Duration
	sizeLimit int

	chDBs      []chDB
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

	chDBs := make([]chDB, 0, len(hostAndPorts))
	for _, hostAndPort := range hostAndPorts {
		db, err := pkgsql.OpenCH("clickhouse", hostAndPort.Host, hostAndPort.Port, cfg.User, cfg.Password)
		if err != nil {
			return nil, errors.Trace(err)
		}
		conn, err := pkgsql.OpenCHDirect(hostAndPort.Host, hostAndPort.Port, cfg.User, cfg.Password)
		db.Driver()
		if err != nil {
			return nil, errors.Trace(err)
		}
		chDBs = append(chDBs, chDB{db, conn})
	}

	e := flashExecutor{
		close:      make(chan bool),
		timeLimit:  timeLimit,
		sizeLimit:  sizeLimit,
		chDBs:      chDBs,
		rowBatches: make(map[string][]*flashRowBatch),
	}

	go e.flushRoutine()

	return &e, nil
}

func (e *flashExecutor) Execute(sqls []string, args [][]interface{}, commitTSs []int64, isDDL bool) error {
	e.Lock()
	defer e.Unlock()

	if e.err != nil {
		log.Errorf("Executor seeing error %v from the flush thread, exiting.", e.err)
		return errors.Trace(e.err)
	}

	if isDDL {
		// Flush all row batches.
		e.flushAll()
		if e.err != nil {
			log.Errorf("Executor seeing error %v when flushing, exiting.", e.err)
			return errors.Trace(e.err)
		}
		for _, chDB := range e.chDBs {
			e.err = pkgsql.ExecuteSQLs(chDB.DB, sqls, args, isDDL)
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
				e.rowBatches[sql] = make([]*flashRowBatch, len(e.chDBs), len(e.chDBs))
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
				e.err = rb.Flush(e.chDBs[hashKey].Conn)
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
	for _, chDB := range e.chDBs {
		err := chDB.DB.Close()
		if err != nil {
			hasError = true
			log.Error(err)
		}
		err = chDB.Conn.Close()
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
		case <-e.close:
			log.Info("Flush thread closing.")
			return
		default:
			e.Lock()
			log.Debug("Flush thread reached time limit, flushing.")
			if e.err != nil {
				e.Unlock()
				log.Errorf("Flush thread seeing error %v from the executor, exiting.", errors.Trace(e.err))
				return
			}
			e.flushAll()
			if e.err != nil {
				e.Unlock()
				log.Errorf("Flush thread seeing error %v when flushing, exiting.", errors.Trace(e.err))
				return
			}
			// TODO: save checkpoint.
			e.Unlock()
		}
	}
}

// partition must be a index of dbs
func (e *flashExecutor) partition(key int64) int {
	return int(key % int64(len(e.chDBs)))
}

func (e *flashExecutor) flushAll() {
	log.Debug(fmt.Sprintf("Flushing all row batches."))
	for _, rbs := range e.rowBatches {
		for i, rb := range rbs {
			err := rb.Flush(e.chDBs[i].Conn)
			if err != nil {
				e.err = errors.Trace(err)
				return
			}
		}
	}
}
