package reparo

import (
	"bufio"
	"hash/crc32"
	"io"
	"net/http"
	// pprof(don't delete me)
	_ "net/http/pprof"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/causality"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	"github.com/pingcap/tidb-binlog/reparo/executor"
	"github.com/pingcap/tidb-binlog/reparo/metrics"
	"github.com/pingcap/tidb-binlog/reparo/translator"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	maxExecutionWaitTime = 100 * time.Millisecond
)

// Reparo is the main part of the restore tool.
type Reparo struct {
	cfg        *Config
	translator translator.Translator
	executors  []executor.Executor
	regexpMap  map[string]*regexp.Regexp
	jobWg      sync.WaitGroup
	jobChs     []chan *job
	causality  *causality.Causality
	wg         sync.WaitGroup
}

// New creates a Reparo object.
func New(cfg *Config) (*Reparo, error) {
	executors, err := createExecutors(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	translator, err := translator.New(cfg.DestType, false, cfg.DestDB)
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Infof("cfg %+v", cfg)
	r := &Reparo{
		cfg:        cfg,
		executors:  executors,
		translator: translator,
		regexpMap:  make(map[string]*regexp.Regexp),
		jobChs:     newJobChans(cfg),
		causality:  causality.NewCausality(),
	}

	return r, nil
}

func (r *Reparo) prepare() error {
	r.GenRegexMap()

	for i := 0; i < r.cfg.WorkerCount; i++ {
		go r.sync(r.executors[i], r.jobChs[i])
	}

	go func() {
		http.Handle("/metrics", prometheus.Handler())
		err := http.ListenAndServe(r.cfg.StatusAddr, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()

	return nil
}

// Process runs the main procedure.
func (r *Reparo) Process() error {
	begin := time.Now()
	if err := r.prepare(); err != nil {
		return errors.Trace(err)
	}
	defer func() {
		closeJobChans(r.jobChs)
	}()

	files, err := r.searchFiles()
	if err != nil {
		return errors.Trace(err)
	}

	var offset int64
	for _, file := range files {
		fd, err := os.OpenFile(file.fullpath, os.O_RDONLY, 0600)
		if err != nil {
			return errors.Annotatef(err, "open file %s error", file.fullpath)
		}
		defer fd.Close()

		offset += file.offset
		ret, err := fd.Seek(file.offset, io.SeekStart)
		if err != nil {
			return errors.Trace(err)
		}
		log.Infof("seek to file %s offset %d got %d", file.fullpath, file.offset, ret)

		br := bufio.NewReader(fd)

		for {
			binlog, length, err := Decode(br)
			if errors.Cause(err) == io.EOF {
				fd.Close()
				log.Infof("read file %s end", file.fullpath)
				offset = 0
				break
			}
			if err != nil {
				return errors.Annotatef(err, "decode binlog error")
			}
			offset += length

			results, isDDL, err := r.Translate(binlog)
			if err != nil {
				return errors.Trace(err)
			}
			if len(results) == 0 {
				continue
			}

			if isDDL {
				r.commitDDLJob(results[0].SQL, results[0].Args, "")
			} else {
				for _, result := range results {
					err = r.commitDMLJob(result.SQL, result.Args, result.Keys)
					if err != nil {
						return errors.Trace(err)
					}
				}
			}

			dt := time.Unix(oracle.ExtractPhysical(uint64(binlog.CommitTs))/1000, 0)
			log.Debugf("offset %d ts %d, datetime %s", offset, binlog.CommitTs, dt.String())
		}
	}

	r.jobWg.Wait()
	log.Infof("[reparo] recovery is done, takes %f seconds", time.Since(begin).Seconds())

	return nil
}

// Close closes the Reparo object.
func (r *Reparo) Close() error {
	r.wg.Wait()
	r.translator.Close()
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
	dmlType opType = iota + 1
	ddlType
	flushType
)

func (r *Reparo) sync(executor executor.Executor, jobChs chan *job) {
	r.wg.Add(1)
	defer r.wg.Done()

	idx := 0
	count := r.cfg.TxnBatch
	sqls := make([]string, 0, count)
	args := make([][]interface{}, 0, count)

	clearF := func() {
		for i := 0; i < idx; i++ {
			r.jobWg.Done()
		}

		idx = 0
		sqls = sqls[0:0]
		args = args[0:0]
	}

	var err error
	for {
		select {
		case job, ok := <-jobChs:
			if !ok {
				return
			}
			idx++

			switch job.binlogTp {
			case ddlType:
				err = executor.Execute([]string{job.sql}, [][]interface{}{job.args}, true)
				if err != nil {
					if !pkgsql.IgnoreDDLError(err) {
						log.Fatalf(errors.ErrorStack(err))
					} else {
						log.Warnf("[ignore ddl error] [sql] %s [args]%v [error]%v", job.sql, job.args, err)
					}
				}
				clearF()

			case flushType:
				err = executor.Execute(sqls, args, false)
				if err != nil {
					log.Fatal(errors.ErrorStack(err))
				}
				clearF()

			case dmlType:
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

		case <-time.After(maxExecutionWaitTime):
			err = executor.Execute(sqls, args, false)
			if err != nil {
				log.Fatalf(errors.ErrorStack(err))
			}
			clearF()
		}
	}
}

func (r *Reparo) addJob(job *job) {
	begin := time.Now()
	switch job.binlogTp {
	case ddlType:
		r.jobWg.Wait()
		dmlCost := time.Since(begin).Seconds()
		if dmlCost > 1 {
			log.Warnf("[reparo] wait dml executed takes %f seconds", dmlCost)
		} else {
			log.Debugf("[reparo] wait dml executed takes %f seconds", dmlCost)
		}
		metrics.WaitExecutedHistogram.WithLabelValues("dml").Observe(dmlCost)
	case flushType:
		r.jobWg.Add(r.cfg.WorkerCount)
		for i := 0; i < r.cfg.WorkerCount; i++ {
			r.jobChs[i] <- job
		}
		r.jobWg.Wait()
		flushCost := time.Since(begin).Seconds()
		if flushCost > 1 {
			log.Warnf("[reparo] wait dml executed takes %f seconds", flushCost)
		} else {
			log.Debugf("[reparo] wait dml executed takes %f seconds", flushCost)
		}
		metrics.WaitExecutedHistogram.WithLabelValues("flush").Observe(flushCost)
		return
	}

	r.jobWg.Add(1)
	idx := int(genHashKey(job.key)) % r.cfg.WorkerCount
	r.jobChs[idx] <- job

	if r.checkWait(job) {
		begin1 := time.Now()
		r.jobWg.Wait()
		ddlCost := time.Since(begin1).Seconds()
		if ddlCost > 1 {
			log.Warnf("[reparo] wait ddl executed takes %f seconds", ddlCost)
		} else {
			log.Debugf("[reparo] wait ddl executed takes %f seconds", ddlCost)
		}
		metrics.WaitExecutedHistogram.WithLabelValues("ddl").Observe(ddlCost)
	}

	totalCost := time.Since(begin).Seconds()
	if totalCost > 1 {
		log.Warnf("[reparo] add job takes %f seconds, is_ddl %v, job %+v", totalCost, job.binlogTp == ddlType, job)
	} else {
		log.Debugf("[reparo] add job takes %f seconds, is_ddl %v, job %+v", totalCost, job.binlogTp == ddlType, job)
	}
	metrics.AddJobHistogram.Observe(totalCost)
}

func (r *Reparo) checkWait(job *job) bool {
	return job.binlogTp == ddlType || job.binlogTp == flushType
}

func (r *Reparo) commitDMLJob(sql string, args []interface{}, keys []string) error {
	key, err := r.resolveCausality(keys)
	if err != nil {
		return errors.Errorf("resolve karam error %v", err)
	}
	job := newDMLJob(sql, args, key)
	r.addJob(job)
	return nil
}

func (r *Reparo) commitDDLJob(sql string, args []interface{}, key string) {
	job := newDDLJob(sql, args, key)
	r.addJob(job)
}

func (r *Reparo) resolveCausality(keys []string) (string, error) {
	begin := time.Now()
	defer func() {
		cost := time.Since(begin).Seconds()
		if cost > 0.1 {
			log.Warnf("[reparo] resolve causality takes %f seconds", cost)
		}
	}()

	if len(keys) == 0 {
		return "", nil
	}

	if r.cfg.DisableCausality {
		return keys[0], nil
	}

	if r.causality.DetectConflict(keys) {
		r.flushJobs()
		r.causality.Reset()
	}

	if err := r.causality.Add(keys); err != nil {
		return "", errors.Trace(err)
	}
	return r.causality.Get(keys[0]), nil
}

func (r *Reparo) flushJobs() {
	log.Info("flush all jobs")
	job := newFlushJob()
	r.addJob(job)
}

func newDDLJob(sql string, args []interface{}, key string) *job {
	return &job{binlogTp: ddlType, sql: sql, args: args, key: key}
}

func newDMLJob(sql string, args []interface{}, key string) *job {
	return &job{binlogTp: dmlType, sql: sql, args: args, key: key}
}

func newFlushJob() *job {
	return &job{binlogTp: flushType}
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

func newJobChans(cfg *Config) []chan *job {
	jobChs := make([]chan *job, 0, cfg.WorkerCount)
	for i := 0; i < cfg.WorkerCount; i++ {
		jobChs = append(jobChs, make(chan *job, cfg.JobChannelSize))
	}
	return jobChs
}

func closeJobChans(jobChs []chan *job) {
	for _, ch := range jobChs {
		close(ch)
	}
}
