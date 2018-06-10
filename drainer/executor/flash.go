package executor

import (
	"database/sql"
	"strconv"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
)

type flashRowBatch struct {
	sql string
	rows []string
}

func newFlashRowBatch(sql string, sizeLimit int) *flashRowBatch {
	return &flashRowBatch{
		sql:           sql,
		rows: make([]string, 0, sizeLimit + 1024), // Loosing the space to tolerant a little more rows being added.
	}
}

// AddRow adds some rows into this row batch.
func (batch *flashRowBatch) AddRow(args []interface{}) error {
	// TODO: really adding rows.
	return nil
}

// Size returns the number of rows stored in this batch.
func (batch *flashRowBatch) Size() int {
	return len(batch.rows)
}

func (batch *flashRowBatch) Flush(db *sql.DB) error {
	// TODO: really flushing rows.
	return nil
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
	for i, hostAndPort := range hostAndPorts {
		dbs[i], err = pkgsql.OpenCH("clickhouse", hostAndPort.Host, hostAndPort.Port, cfg.User, cfg.Password)
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
		return errors.Trace(e.err)
	}

	if isDDL {
		// Flush all row batches.
		e.flushAll()
		if e.err != nil {
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
				e.rowBatches[sql] = make([]*flashRowBatch, 0, len(e.dbs))
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
	for {
		time.Sleep(e.timeLimit * time.Second)
		select {
		case <- e.close:
			return
		default:
			e.Lock()
			if e.err != nil {
				e.Unlock()
				return
			}
			e.flushAll()
			if e.err != nil {
				e.Unlock()
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
	for _, rbs := range e.rowBatches {
		for i, rb := range rbs {
			e.err = rb.Flush(e.dbs[i])
			if e.err != nil {
				return
			}
		}
	}
}
