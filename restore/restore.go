package restore

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/causality"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	"github.com/pingcap/tidb-binlog/restore/executor"
	tbl "github.com/pingcap/tidb-binlog/restore/table"
	"github.com/pingcap/tidb-binlog/restore/translator"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

const (
	jobChanSize = 1000

	maxExecutionWaitTime = 3 * time.Second
	executionWaitTime    = 10 * time.Millisecond
)

// Restore i the main part of the restore tool.
type Restore struct {
	cfg        *Config
	translator translator.Translator
	executors  []executor.Executor
	regexMap   map[string]*regexp.Regexp
	jobWg      sync.WaitGroup
	jobCh      []chan *job
	c          *causality.Causality
	wg         sync.WaitGroup

	tables map[string]*tbl.Table
	db     *sql.DB
}

// New creates a Restore object.
func New(cfg *Config) (*Restore, error) {
	executors, err := createExecutors(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	db, err := pkgsql.OpenDB("mysql", cfg.DestDB.Host, cfg.DestDB.Port, cfg.DestDB.User, cfg.DestDB.Password)
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Infof("cfg %+v", cfg)
	return &Restore{
		cfg:        cfg,
		translator: translator.New(cfg.DestType, false),
		executors:  executors,
		regexMap:   make(map[string]*regexp.Regexp),
		jobCh:      newJobChans(cfg.WorkerCount),
		c:          causality.NewCausality(),
		tables:     make(map[string]*tbl.Table),
		db:         db,
	}, nil
}

func (r *Restore) prepare() error {
	r.GenRegexMap()

	for i := 0; i < r.cfg.WorkerCount; i++ {
		go r.sync(r.executors[i], r.jobCh[i])
	}

	return nil
}

// Process runs the main procedure.
func (r *Restore) Process() error {
	if err := r.prepare(); err != nil {
		return errors.Trace(err)
	}

	dir := r.cfg.Dir
	files, err := r.searchFiles(dir)
	if err != nil {
		return errors.Trace(err)
	}
	codec := compress.ToCompressionCodec(r.cfg.Compression)

	for _, file := range files {
		fd, err := os.OpenFile(file.fullpath, os.O_RDONLY, 0600)
		if err != nil {
			return errors.Annotatef(err, "open file %s error", file.fullpath)
		}
		defer fd.Close()

		ret, err := fd.Seek(file.offset, io.SeekStart)
		if err != nil {
			return errors.Trace(err)
		}
		log.Infof("seek to file %s offset %d got %d", file.fullpath, file.offset, ret)

		br := bufio.NewReader(fd)
		var rd io.Reader

		switch codec {
		case compress.CompressionNone:
			rd = br
		case compress.CompressionGZIP:
			gzr, err := gzip.NewReader(br)
			if err == io.EOF {
				log.Infof("EOF")
				continue
			}
			if err != nil {
				return errors.Trace(err)
			}
			rd = gzr
			defer gzr.Close()
		}

		for {
			binlog, err := Decode(rd)
			if errors.Cause(err) == io.EOF {
				if gzr, ok := rd.(*gzip.Reader); ok {
					gzr.Close()
				}
				fd.Close()
				log.Infof("read file %s end", file.fullpath)
				break
			}
			if err != nil {
				return errors.Annotatef(err, "decode binlog error")
			}

			sqls, args, isDDL, err := r.Translate(binlog)
			if err != nil {
				return errors.Trace(err)
			}
			if len(sqls) == 0 {
				continue
			}

			// TODO: send to channel.
			// if isDDL {
			// 	newDDLJob(sqls[0], args[0], "")
			// } else {
			// 	for i, sql := range sqls {
			// 		// r.commitDMLJob(sql, args[i], "")
			// 	}
			// }

			ret, err := fd.Seek(0, io.SeekCurrent)
			if err != nil {
				return errors.Trace(err)
			}
			dt := time.Unix(oracle.ExtractPhysical(uint64(binlog.CommitTs))/1000, 0)
			log.Infof("offset %d ts %d, datetime %s", ret, binlog.CommitTs, dt.String())
		}
	}

	return nil
}

// Close closes the Restore object.
func (r *Restore) Close() error {
	r.db.Close()
	closeExecutors(r.executors)
	return nil
}

type job struct {
	binlogTp opType
	sql      string
	args     []interface{}
	key      string
}

type opType byte

const (
	dml opType = iota + 1
	ddl
)

func (r *Restore) sync(executor executor.Executor, jobCh chan *job) {
	r.wg.Add(1)
	defer r.wg.Done()

	idx := 0
	count := r.cfg.TxnBatch
	sqls := make([]string, 0, count)
	args := make([][]interface{}, 0, count)
	lastSyncTime := time.Now()

	clearF := func() {
		for i := 0; i < idx; i++ {
			r.jobWg.Done()
		}

		idx = 0
		sqls = sqls[0:0]
		args = args[0:0]
		lastSyncTime = time.Now()
	}

	var err error
	for {
		select {
		case job, ok := <-jobCh:
			if !ok {
				return
			}
			idx++
			if job.binlogTp == ddl {
				err = executor.Execute([]string{job.sql}, [][]interface{}{job.args}, true)
				if err != nil {
					if !pkgsql.IgnoreDDLError(err) {
						log.Fatalf(errors.ErrorStack(err))
					} else {
						log.Warnf("[ignore ddl error] [sql] %s [args]%v [error]%v", job.sql, job.args, err)
					}
					clearF()
				}
			} else {
				sqls = append(sqls, job.sql)
				args = append(args, job.args)
			}

			if idx >= count {
				err = executor.Execute(sqls, args, false)
				if err != nil {
					log.Fatal(errors.ErrorStack(err))
				}
				clearF()
			}

		default:
			if time.Since(lastSyncTime) >= maxExecutionWaitTime {
				err = executor.Execute(sqls, args, false)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}
				clearF()
			}
			time.Sleep(executionWaitTime)
		}
	}
}

func (r *Restore) addJob(job *job) {
	if job.binlogTp == ddl {
		r.jobWg.Wait()
	}

	r.jobWg.Add(1)
	idx := int(genHashKey(fmt.Sprintf("%s", job.key))) % r.cfg.WorkerCount
	r.jobCh[idx] <- job

	if r.checkWait(job) {
		r.jobWg.Wait()
	}
}

func (r *Restore) checkWait(job *job) bool {
	return job.binlogTp == ddl
}

func (r *Restore) commitDMLJob(sql string, args []interface{}, keys []string) error {
	key, err := r.resolveCausality(keys)
	if err == nil {
		return errors.Errorf("resolve karam error %v", err)
	}
	job := newDMLJob(sql, args, key)
	r.addJob(job)
	return nil
}

func (r *Restore) resolveCausality(keys []string) (string, error) {
	if r.cfg.DisableCausality {
		if len(keys) > 0 {
			return keys[0], nil
		}
		return "", nil
	}

	if r.c.DetectConflict(keys) {
		r.c.Reset()
	}

	if err := r.c.Add(keys); err != nil {
		return "", errors.Trace(err)
	}
	var key string
	if len(keys) > 0 {
		key = keys[0]
	}
	return r.c.Get(key), nil
}

func newDDLJob(sql string, args []interface{}, key string) *job {
	return &job{binlogTp: ddl, sql: sql, args: args, key: key}
}

func newDMLJob(sql string, args []interface{}, key string) *job {
	return &job{binlogTp: dml, sql: sql, args: args, key: key}
}

func genHashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func createExecutors(cfg *Config) ([]executor.Executor, error) {
	executors := make([]executor.Executor, 0, cfg.WorkerCount)
	for i := 0; i < cfg.WorkerCount; i++ {
		executor, err := executor.New(cfg.DestType, cfg.DestDB)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executors = append(executors, executor)
	}

	return executors, nil
}

func closeExecutors(executors []executor.Executor) {
	for _, e := range executors {
		if err := e.Close(); err != nil {
			log.Errorf("close executors failed - %v", err)
		}
	}
}

func newJobChans(count int) []chan *job {
	jobChs := make([]chan *job, 0, count)
	for i := 0; i < count; i++ {
		jobChs = append(jobChs, make(chan *job, jobChanSize))
	}
	return jobChs
}
