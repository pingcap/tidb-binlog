package drainer

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/drainer/executor"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv/oracle"
	pb "github.com/pingcap/tipb/go-binlog"
)

var (
	executionWaitTime    = 10 * time.Millisecond
	maxExecutionWaitTime = 3 * time.Second

	workerMetricsLimit = 10
)

// Syncer converts tidb binlog to the specified DB sqls, and sync it to target DB
type Syncer struct {
	schema *Schema
	cp     checkpoint.CheckPoint

	cfg *SyncerConfig

	translator translator.SQLTranslator

	wg sync.WaitGroup

	input chan *binlogItem
	jobWg sync.WaitGroup
	jobCh []chan *job

	executors []executor.Executor

	positions    map[string]int64
	initCommitTS int64

	// because TiDB is case-insensitive, only lower-case here.
	ignoreSchemaNames map[string]struct{}

	ctx    context.Context
	cancel context.CancelFunc

	filter *filter

	causality *causality

	lastSyncTime time.Time
}

// NewSyncer returns a Drainer instance
func NewSyncer(ctx context.Context, cp checkpoint.CheckPoint, cfg *SyncerConfig) (*Syncer, error) {
	syncer := new(Syncer)
	syncer.cfg = cfg
	syncer.cp = cp
	syncer.input = make(chan *binlogItem, maxBinlogItemCount)
	syncer.jobCh = newJobChans(cfg.WorkerCount)
	syncer.ctx, syncer.cancel = context.WithCancel(ctx)
	syncer.initCommitTS = cp.TS()
	syncer.positions = make(map[string]int64)
	syncer.causality = newCausality()
	syncer.lastSyncTime = time.Now()
	syncer.filter = newFilter(formatIgnoreSchemas(cfg.IgnoreSchemas), cfg.DoDBs, cfg.DoTables)

	return syncer, nil
}

func newJobChans(count int) []chan *job {
	jobCh := make([]chan *job, 0, count)
	size := maxBinlogItemCount / count
	for i := 0; i < count; i++ {
		jobCh = append(jobCh, make(chan *job, size))
	}

	return jobCh
}

func closeJobChans(jobChs []chan *job) {
	for _, ch := range jobChs {
		close(ch)
	}
}

// Start starts to sync.
func (s *Syncer) Start(jobs []*model.Job) error {
	err := s.run(jobs)

	return errors.Trace(err)
}

func (s *Syncer) addDMLCount(tp pb.MutationType, nums int) {
	switch tp {
	case pb.MutationType_Insert:
		eventCounter.WithLabelValues("Insert").Add(float64(nums))
	case pb.MutationType_Update:
		eventCounter.WithLabelValues("Update").Add(float64(nums))
	case pb.MutationType_DeleteRow:
		eventCounter.WithLabelValues("Delete").Add(float64(nums))
	}
}

func (s *Syncer) addDDLCount() {
	eventCounter.WithLabelValues("DDL").Add(1)
}

func (s *Syncer) checkWait(job *job) bool {
	if !job.isCompleteBinlog {
		return false
	}

	if s.cp.Check(job.commitTS) {
		return true
	}

	if job.binlogTp == translator.DDL {
		return true
	}

	return false
}

func (s *Syncer) enableSafeModeInitializationPhase() {
	// set safeMode to true and useInsert to flase at the first, and will use the config after 5 minutes.
	s.translator.SetConfig(true)

	go func() {
		ctx, cancel := context.WithCancel(s.ctx)
		defer func() {
			cancel()
			s.translator.SetConfig(s.cfg.SafeMode)
		}()

		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Minute):
		}
	}()
}

type job struct {
	binlogTp   translator.OpType
	mutationTp pb.MutationType
	sql        string
	args       []interface{}
	key        string
	commitTS   int64
	nodeID     string
	// set to true when this is the last job of a txn
	// we will generate many job and call addJob(job) from a txn
	isCompleteBinlog bool
}

func (j *job) String() string {
	// build args String, avoid to print too big value arg
	builder := new(strings.Builder)
	builder.WriteString("[")

	for i := 0; i < len(j.args); i++ {
		if i > 0 {
			builder.WriteString(", ")
		}

		tmp := fmt.Sprintf("%v", j.args[i])
		if len(tmp) > 30 {
			tmp = tmp[0:30] + "..."
		}
		builder.WriteString(tmp)
	}
	builder.WriteString("]")

	return fmt.Sprintf("{binlogTp: %v, mutationTp: %v, sql: %v, args: %v, key: %v, commitTS: %v, nodeID: %v, isCompleteBinlog: %v}", j.binlogTp, j.mutationTp, j.sql, builder.String(), j.key, j.commitTS, j.nodeID, j.isCompleteBinlog)
}

func newDMLJob(tp pb.MutationType, sql string, args []interface{}, key string, commitTS int64, nodeID string, isCompleteBinlog bool) *job {
	return &job{binlogTp: translator.DML, mutationTp: tp, sql: sql, args: args, key: key, commitTS: commitTS, nodeID: nodeID, isCompleteBinlog: isCompleteBinlog}
}

func newDDLJob(sql string, args []interface{}, key string, commitTS int64, nodeID string) *job {
	return &job{binlogTp: translator.DDL, sql: sql, args: args, key: key, commitTS: commitTS, nodeID: nodeID, isCompleteBinlog: true}
}

func newFakeJob(commitTS int64, nodeID string) *job {
	return &job{binlogTp: translator.FAKE, commitTS: commitTS, nodeID: nodeID, isCompleteBinlog: true}
}

func workerName(idx int) string {
	return fmt.Sprintf("worker_%d", idx)
}

func (s *Syncer) addJob(job *job) {
	log.Debugf("add job: %s", job)

	// make all DMLs be executed before DDL
	if job.binlogTp == translator.DDL {
		s.jobWg.Wait()
	} else if job.binlogTp == translator.FLUSH {
		s.jobWg.Add(s.cfg.WorkerCount)
		for i := 0; i < s.cfg.WorkerCount; i++ {
			s.jobCh[i] <- job
		}
		eventCounter.WithLabelValues("flush").Add(1)
		s.jobWg.Wait()
		return
	}

	if job.binlogTp != translator.FAKE {
		s.jobWg.Add(1)
		idx := int(genHashKey(job.key)) % s.cfg.WorkerCount
		s.jobCh[idx] <- job
	}

	if pos, ok := s.positions[job.nodeID]; !ok || job.commitTS > pos {
		s.positions[job.nodeID] = job.commitTS
	}

	wait := s.checkWait(job)
	if wait {
		eventCounter.WithLabelValues("savepoint").Add(1)
		s.jobWg.Wait()
		s.causality.reset()
		s.savePoint(job.commitTS)
	}
}

func (s *Syncer) commitJob(tp pb.MutationType, sql string, args []interface{}, keys []string, commitTS int64, nodeID string, isCompleteBinlog bool) error {
	key, err := s.resolveCasuality(keys)
	if err != nil {
		return errors.Errorf("resolve karam error %v", err)
	}

	job := newDMLJob(tp, sql, args, key, commitTS, nodeID, isCompleteBinlog)
	log.Debugf("keys: %v, dispatch key: %v", keys, key)

	s.addJob(job)
	return nil
}

func (s *Syncer) resolveCasuality(keys []string) (string, error) {
	if len(keys) == 0 {
		return "", nil
	}

	if s.cfg.DisableCausality {
		return keys[0], nil
	}

	if s.causality.detectConflict(keys) {
		if err := s.flushJobs(); err != nil {
			return "", errors.Trace(err)
		}
		s.causality.reset()
	}

	if err := s.causality.add(keys); err != nil {
		return "", errors.Trace(err)
	}

	return s.causality.get(keys[0]), nil
}

func (s *Syncer) flushJobs() error {
	log.Infof("flush all jobs checkpoint = %v", s.cp)
	job := &job{binlogTp: translator.FLUSH}
	s.addJob(job)
	return nil
}

func (s *Syncer) savePoint(ts int64) {
	if ts < s.cp.TS() {
		log.Errorf("ts %d is less than checkpoint ts %d", ts, s.cp.TS())
	}

	log.Infof("[write save point]%d", ts)
	err := s.cp.Save(ts)
	if err != nil {
		log.Fatalf("[write save point]%d[error]%v", ts, err)
	}

	checkpointTSOGauge.Set(float64(oracle.ExtractPhysical(uint64(ts))))
}

func (s *Syncer) sync(executor executor.Executor, jobChan chan *job, executorIdx int) {
	s.wg.Add(1)
	defer s.wg.Done()

	idx := 0
	count := s.cfg.TxnBatch
	sqls := make([]string, 0, count)
	args := make([][]interface{}, 0, count)
	commitTSs := make([]int64, 0, count)
	tpCnt := make(map[pb.MutationType]int)
	lastSyncTime := time.Now()

	clearF := func() {
		for i := 0; i < idx; i++ {
			s.jobWg.Done()
		}

		idx = 0
		sqls = sqls[0:0]
		args = args[0:0]
		commitTSs = commitTSs[0:0]
		s.lastSyncTime = time.Now()
		lastSyncTime = time.Now()
		for tpName, v := range tpCnt {
			s.addDMLCount(tpName, v)
			tpCnt[tpName] = 0
		}
	}

	workerName := workerName(executorIdx % workerMetricsLimit)
	var err error
	for {
		select {
		case job, ok := <-jobChan:
			if !ok {
				return
			}
			idx++

			qsize := len(jobChan)
			queueSizeGauge.WithLabelValues(workerName).Set(float64(qsize))

			if job.binlogTp == translator.DDL {
				// compute txn duration
				err = execute(executor, []string{job.sql}, [][]interface{}{job.args}, []int64{job.commitTS}, true)
				if err != nil {
					if !pkgsql.IgnoreDDLError(err) {
						log.Fatalf(errors.ErrorStack(err))
					} else {
						log.Warnf("[ignore ddl error][sql]%s[args]%v[error]%v", job.sql, job.args, err)
					}
				}
				s.addDDLCount()
				clearF()
			} else if job.binlogTp == translator.DML {
				sqls = append(sqls, job.sql)
				args = append(args, job.args)
				commitTSs = append(commitTSs, job.commitTS)
				tpCnt[job.mutationTp]++
			}

			if job.binlogTp == translator.FLUSH ||
				!s.cfg.DisableDispatch && idx >= count ||
				s.cfg.DisableDispatch && job.isCompleteBinlog {
				err = execute(executor, sqls, args, commitTSs, false)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}
				clearF()
			}

		default:
			now := time.Now()
			if now.Sub(lastSyncTime) >= maxExecutionWaitTime && !s.cfg.DisableDispatch {
				err = execute(executor, sqls, args, commitTSs, false)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}
				clearF()
			}

			time.Sleep(executionWaitTime)
		}
	}
}

func (s *Syncer) run(jobs []*model.Job) error {
	s.wg.Add(1)
	defer func() {
		closeJobChans(s.jobCh)
		s.wg.Done()
	}()

	var err error

	for i := 0; i < len(jobs); i++ {
		data, err := json.Marshal(jobs[i])
		if err != nil {
			log.Error(err)
		} else {
			log.Debug("get ddl binlog job: ", string(data))
		}
	}
	s.schema, err = NewSchema(jobs, s.cfg.DestDBType == "tidb")

	s.executors, err = createExecutors(s.cfg.DestDBType, s.cfg.To, s.cfg.WorkerCount)
	if err != nil {
		return errors.Trace(err)
	}

	s.translator, err = translator.New(s.cfg.DestDBType)
	if err != nil {
		return errors.Trace(err)
	}

	s.translator.SetConfig(s.cfg.SafeMode)
	go s.enableSafeModeInitializationPhase()

	for i := 0; i < s.cfg.WorkerCount; i++ {
		go s.sync(s.executors[i], s.jobCh[i], i)
	}

	var b *binlogItem
	for {
		select {
		case <-s.ctx.Done():
			return nil
		case b = <-s.input:
			queueSizeGauge.WithLabelValues("syncer_input").Set(float64(len(s.input)))
			log.Debugf("consume binlogItem: %s", b)
		}

		binlog := b.binlog
		startTS := binlog.GetStartTs()
		commitTS := binlog.GetCommitTs()
		jobID := binlog.GetDdlJobId()

		if startTS == commitTS {
			// generate fake binlog job
			s.addJob(newFakeJob(commitTS, b.nodeID))

		} else if jobID == 0 {
			preWriteValue := binlog.GetPrewriteValue()
			preWrite := &pb.PrewriteValue{}
			err = preWrite.Unmarshal(preWriteValue)
			if err != nil {
				return errors.Errorf("prewrite %s unmarshal error %v", preWriteValue, err)
			}

			log.Debug("DML SchemaVersion: ", preWrite.SchemaVersion)
			err = s.schema.handlePreviousDDLJobIfNeed(preWrite.SchemaVersion)
			if err != nil {
				return errors.Trace(err)
			}

			err = s.translateSqls(preWrite.GetMutations(), commitTS, b.nodeID)
			if err != nil {
				return errors.Trace(err)
			}
		} else if jobID > 0 {
			log.Debug("get ddl binlog job: ", b.job)

			// Notice: the version of DDL Binlog we receive are Monotonically increasing
			// DDL (with version 10, commit ts 100) -> DDL (with version 9, commit ts 101) would never happen
			s.schema.addJob(b.job)

			log.Debug("DDL SchemaVersion: ", b.job.BinlogInfo.SchemaVersion)
			err = s.schema.handlePreviousDDLJobIfNeed(b.job.BinlogInfo.SchemaVersion)
			if err != nil {
				return errors.Trace(err)
			}

			log.Debug("ddl query: ", b.job.Query)
			sql := b.job.Query
			schema, table, err := s.schema.getSchemaTableAndDelete(b.job.BinlogInfo.SchemaVersion)
			if err != nil {
				return errors.Trace(err)
			}

			if s.filter.skipSchemaAndTable(schema, table) {
				log.Infof("[skip ddl]db:%s table:%s, sql:%s, commit ts %d", schema, table, sql, commitTS)
			} else if sql != "" {
				sql, err = s.translator.GenDDLSQL(sql, schema, commitTS)
				if err != nil {
					return errors.Trace(err)
				}

				log.Infof("[ddl][start]%s[commit ts]%v", sql, commitTS)
				var args []interface{}
				// for kafka, we want to know the relate schema and table, get it while args now
				// in executor
				if s.cfg.DestDBType == "kafka" {
					args = []interface{}{schema, table}
				}
				job := newDDLJob(sql, args, "", commitTS, b.nodeID)
				s.addJob(job)
				log.Infof("[ddl][end]%s[commit ts]%v", sql, commitTS)
			}
		}

	}
}

func (s *Syncer) translateSqls(mutations []pb.TableMutation, commitTS int64, nodeID string) error {
	useMysqlProtocol := (s.cfg.DestDBType == "tidb" || s.cfg.DestDBType == "mysql")

	for mutationIdx, mutation := range mutations {
		isLastMutation := (mutationIdx == len(mutations)-1)

		table, ok := s.schema.TableByID(mutation.GetTableId())
		if !ok {
			return errors.Errorf("not found table id: %d", mutation.GetTableId())
		}

		schemaName, tableName, ok := s.schema.SchemaAndTableName(mutation.GetTableId())
		if !ok {
			return errors.Errorf("not found table id: %d", mutation.GetTableId())
		}

		if s.filter.skipSchemaAndTable(schemaName, tableName) {
			log.Debugf("[skip dml]db:%s table:%s", schemaName, tableName)
			continue
		}

		var (
			safeMode bool

			err  error
			sqls = make(map[pb.MutationType][]string)

			// the dispatch keys
			keys = make(map[pb.MutationType][][]string)

			// the restored sqls's args, ditto
			args = make(map[pb.MutationType][][]interface{})

			// the offset of specified type sql
			offsets = make(map[pb.MutationType]int)

			// the binlog dml sort
			sequences = mutation.GetSequence()
		)

		if len(mutation.GetInsertedRows()) > 0 {
			sqls[pb.MutationType_Insert], keys[pb.MutationType_Insert], args[pb.MutationType_Insert], err = s.translator.GenInsertSQLs(schemaName, table, mutation.GetInsertedRows(), commitTS)
			if err != nil {
				return errors.Errorf("gen insert sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}
			offsets[pb.MutationType_Insert] = 0
		}

		if len(mutation.GetUpdatedRows()) > 0 {
			sqls[pb.MutationType_Update], keys[pb.MutationType_Update], args[pb.MutationType_Update], safeMode, err = s.translator.GenUpdateSQLs(schemaName, table, mutation.GetUpdatedRows(), commitTS)
			if err != nil {
				return errors.Errorf("gen update sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}
			offsets[pb.MutationType_Update] = 0
		}

		if len(mutation.GetDeletedRows()) > 0 {
			sqls[pb.MutationType_DeleteRow], keys[pb.MutationType_DeleteRow], args[pb.MutationType_DeleteRow], err = s.translator.GenDeleteSQLs(schemaName, table, mutation.GetDeletedRows(), commitTS)
			if err != nil {
				return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}
			offsets[pb.MutationType_DeleteRow] = 0
		}

		for idx, dmlType := range sequences {
			if offsets[dmlType] >= len(sqls[dmlType]) {
				return errors.Errorf("gen sqls failed: sequence %v execution %s sqls %v", sequences, dmlType, sqls[dmlType])
			}

			isCompleteBinlog := (idx == len(sequences)-1) && isLastMutation

			// update is split to delete and insert
			if dmlType == pb.MutationType_Update && safeMode && useMysqlProtocol {
				err = s.commitJob(pb.MutationType_DeleteRow, sqls[dmlType][offsets[dmlType]], args[dmlType][offsets[dmlType]], keys[dmlType][offsets[dmlType]], commitTS, nodeID, false)
				if err != nil {
					return errors.Trace(err)
				}

				err = s.commitJob(pb.MutationType_Insert, sqls[dmlType][offsets[dmlType]+1], args[dmlType][offsets[dmlType]+1], keys[dmlType][offsets[dmlType]+1], commitTS, nodeID, isCompleteBinlog)
				if err != nil {
					return errors.Trace(err)
				}
				offsets[dmlType] = offsets[dmlType] + 2
			} else {
				err = s.commitJob(dmlType, sqls[dmlType][offsets[dmlType]], args[dmlType][offsets[dmlType]], keys[dmlType][offsets[dmlType]], commitTS, nodeID, isCompleteBinlog)
				if err != nil {
					return errors.Trace(err)
				}
				offsets[dmlType] = offsets[dmlType] + 1
			}
		}

		for tp := range sqls {
			if offsets[tp] != len(sqls[tp]) {
				return errors.Errorf("binlog is corruption, item %v", mutations)
			}
		}
	}

	return nil
}

// Add adds binlogItem to the syncer's input channel
func (s *Syncer) Add(b *binlogItem) {
	select {
	case <-s.ctx.Done():
	case s.input <- b:
		log.Debugf("receive publish binlog item: %s", b)
	}
}

// Close closes syncer.
func (s *Syncer) Close() {
	log.Debug("closing syncer")
	s.cancel()
	s.wg.Wait()
	closeExecutors(s.executors...)
	log.Debug("syncer is closed")
}

// GetLastSyncTime returns lastSyncTime
func (s *Syncer) GetLastSyncTime() time.Time {
	return s.lastSyncTime
}

// GetLatestCommitTS returns the latest commit ts.
func (s *Syncer) GetLatestCommitTS() int64 {
	return s.cp.TS()
}
