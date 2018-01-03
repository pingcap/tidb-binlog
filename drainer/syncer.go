package drainer

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/drainer/executor"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb/model"
	pb "github.com/pingcap/tipb/go-binlog"
)

var (
	maxDMLRetryCount = 100
	maxDDLRetryCount = 5

	executionWaitTime    = 10 * time.Millisecond
	maxExecutionWaitTime = 3 * time.Second
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

	positions    map[string]pb.Pos
	initCommitTS int64

	// because TiDB is case-insensitive, only lower-case here.
	ignoreSchemaNames map[string]struct{}

	ctx    context.Context
	cancel context.CancelFunc

	reMap map[string]*regexp.Regexp

	c *causality
}

// NewSyncer returns a Drainer instance
func NewSyncer(ctx context.Context, cp checkpoint.CheckPoint, cfg *SyncerConfig) (*Syncer, error) {
	syncer := new(Syncer)
	syncer.cfg = cfg
	syncer.ignoreSchemaNames = formatIgnoreSchemas(cfg.IgnoreSchemas)
	syncer.cp = cp
	syncer.input = make(chan *binlogItem, maxBinlogItemCount)
	syncer.jobCh = newJobChans(cfg.WorkerCount)
	syncer.reMap = make(map[string]*regexp.Regexp)
	syncer.ctx, syncer.cancel = context.WithCancel(ctx)
	syncer.initCommitTS, _ = cp.Pos()
	syncer.positions = make(map[string]pb.Pos)
	syncer.c = newCausality()

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
	// prepare schema for work
	b, err := s.prepare(jobs)
	if err != nil || b == nil {
		return errors.Trace(err)
	}

	err = s.run(b)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// the binlog maybe not complete before the initCommitTS, so we should ignore them.
// at the same time, we try to find the latest schema version before the initCommitTS to reconstruct local schemas.
func (s *Syncer) prepare(jobs []*model.Job) (*binlogItem, error) {
	var latestSchemaVersion int64
	var schemaVersion int64
	var b *binlogItem
	var err error

	for {
		select {
		case <-s.ctx.Done():
			return nil, nil
		case b = <-s.input:
		}

		binlog := b.binlog
		commitTS := binlog.GetCommitTs()
		jobID := binlog.GetDdlJobId()

		if jobID == 0 {
			preWriteValue := binlog.GetPrewriteValue()
			preWrite := &pb.PrewriteValue{}
			err = preWrite.Unmarshal(preWriteValue)
			if err != nil {
				return nil, errors.Errorf("prewrite %s unmarshal error %v", preWriteValue, err)
			}
			schemaVersion = preWrite.GetSchemaVersion()
		} else {
			schemaVersion = b.job.BinlogInfo.SchemaVersion
		}
		if schemaVersion > latestSchemaVersion {
			latestSchemaVersion = schemaVersion
		}

		if commitTS <= s.initCommitTS {
			continue
		}

		if jobID > 0 {
			latestSchemaVersion = b.job.BinlogInfo.SchemaVersion - 1
		}
		// find all ddl job that need to reconstruct local schemas
		var exceptedJobs []*model.Job
		for _, job := range jobs {
			if job.BinlogInfo.SchemaVersion <= latestSchemaVersion {
				exceptedJobs = append(exceptedJobs, job)
			}
		}

		s.schema, err = NewSchema(exceptedJobs, s.ignoreSchemaNames)
		if err != nil {
			return nil, errors.Trace(err)
		}

		return b, nil
	}
}

// handleDDL has four return values,
// the first value[string]: the schema name
// the second value[string]: the table name
// the third value[string]: the sql that is corresponding to the job
// the fourth value[error]: the handleDDL execution's err
func (s *Syncer) handleDDL(job *model.Job) (string, string, string, error) {
	if job.State == model.JobStateCancelled {
		return "", "", "", nil
	}

	log.Infof("ddl query %s", job.Query)
	sql := job.Query
	if sql == "" {
		return "", "", "", errors.Errorf("[ddl job sql miss]%+v", job)
	}

	switch job.Type {
	case model.ActionCreateSchema:
		// get the DBInfo from job rawArgs
		schema := job.BinlogInfo.DBInfo
		if filterIgnoreSchema(schema, s.ignoreSchemaNames) {
			s.schema.AddIgnoreSchema(schema)
			return "", "", "", nil
		}

		err := s.schema.CreateSchema(schema)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.O, "", sql, nil

	case model.ActionDropSchema:
		_, ok := s.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			s.schema.DropIgnoreSchema(job.SchemaID)
			return "", "", "", nil
		}

		schemaName, err := s.schema.DropSchema(job.SchemaID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schemaName, "", sql, nil

	case model.ActionRenameTable:
		// ignore schema doesn't support reanme ddl
		_, ok := s.schema.SchemaByTableID(job.TableID)
		if !ok {
			return "", "", "", errors.NotFoundf("table(%d) or it's schema", job.TableID)
		}
		_, ok = s.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", "", errors.Errorf("ignore schema %d doesn't support rename ddl sql %s", job.SchemaID, sql)
		}
		// first drop the table
		_, err := s.schema.DropTable(job.TableID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}
		// create table
		table := job.BinlogInfo.TableInfo
		schema, ok := s.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err = s.schema.CreateTable(schema, table)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.O, table.Name.O, sql, nil

	case model.ActionCreateTable:
		table := job.BinlogInfo.TableInfo
		if table == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}

		_, ok := s.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", "", nil
		}

		schema, ok := s.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err := s.schema.CreateTable(schema, table)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.O, table.Name.O, sql, nil

	case model.ActionDropTable:
		_, ok := s.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", "", nil
		}

		schema, ok := s.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		tableName, err := s.schema.DropTable(job.TableID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.O, tableName, sql, nil

	case model.ActionTruncateTable:
		_, ok := s.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", "", nil
		}

		schema, ok := s.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		_, err := s.schema.DropTable(job.TableID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		table := job.BinlogInfo.TableInfo
		if table == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}

		err = s.schema.CreateTable(schema, table)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.O, table.Name.O, sql, nil

	default:
		tbInfo := job.BinlogInfo.TableInfo
		if tbInfo == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}

		_, ok := s.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", "", nil
		}

		schema, ok := s.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err := s.schema.ReplaceTable(tbInfo)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.O, tbInfo.Name.O, sql, nil
	}
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
	if job.binlogTp == translator.DDL || job.binlogTp == translator.FLUSH {
		return true
	}
	if (!s.cfg.DisableDispatch || job.isCompleteBinlog) && s.cp.Check() {
		return true
	}
	return false
}

type job struct {
	binlogTp         translator.OpType
	mutationTp       pb.MutationType
	sql              string
	args             []interface{}
	key              string
	commitTS         int64
	pos              pb.Pos
	nodeID           string
	isCompleteBinlog bool
}

func newDMLJob(tp pb.MutationType, sql string, args []interface{}, key string, commitTS int64, pos pb.Pos, nodeID string) *job {
	return &job{binlogTp: translator.DML, mutationTp: tp, sql: sql, args: args, key: key, commitTS: commitTS, pos: pos, nodeID: nodeID}
}

func newDDLJob(sql string, args []interface{}, key string, commitTS int64, pos pb.Pos, nodeID string) *job {
	return &job{binlogTp: translator.DDL, sql: sql, args: args, key: key, commitTS: commitTS, pos: pos, nodeID: nodeID}
}

// binlog bounadary job is used to group jobs, like a barrier
func newBinlogBoundaryJob(commitTS int64, pos pb.Pos, nodeID string) *job {
	return &job{binlogTp: translator.DML, commitTS: commitTS, pos: pos, nodeID: nodeID, isCompleteBinlog: true}
}

func (s *Syncer) addJob(job *job) {
	// make all DMLs be executed before DDL
	if job.binlogTp == translator.DDL {
		s.jobWg.Wait()
	} else if job.binlogTp == translator.FLUSH {
		s.jobWg.Wait()
		return
	}

	s.jobWg.Add(1)
	idx := int(genHashKey(fmt.Sprintf("%v", job.key))) % s.cfg.WorkerCount
	s.jobCh[idx] <- job

	if pos, ok := s.positions[job.nodeID]; !ok || ComparePos(job.pos, pos) > 0 {
		s.positions[job.nodeID] = job.pos
	}

	wait := s.checkWait(job)
	if wait {
		s.jobWg.Wait()
		s.savePoint(job.commitTS, s.positions)
	}
}

func (s *Syncer) commitJob(tp pb.MutationType, sql string, args []interface{}, keys []string, commitTS int64, pos pb.Pos, nodeID string) error {
	key, err := s.resolveCasuality(keys)
	if err != nil {
		return errors.Errorf("resolve karam error %v", err)
	}
	job := newDMLJob(tp, sql, args, key, commitTS, pos, nodeID)
	s.addJob(job)
	return nil
}

func (s *Syncer) resolveCasuality(keys []string) (string, error) {
	if s.cfg.DisableCausality {
		if len(keys) > 0 {
			return keys[0], nil
		}
		return "", nil
	}
	if s.c.detectConflict(keys) {
		if err := s.flushJobs(); err != nil {
			return "", errors.Trace(err)
		}
		s.c.reset()
	}
	if err := s.c.add(keys); err != nil {
		return "", errors.Trace(err)
	}
	var key string
	if len(keys) > 0 {
		key = keys[0]
	}
	return s.c.get(key), nil
}

func (s *Syncer) flushJobs() error {
	log.Infof("flush all jobs checkpoint = %v", s.cp)
	job := &job{binlogTp: translator.FLUSH}
	s.addJob(job)
	return nil
}

func (s *Syncer) savePoint(ts int64, positions map[string]pb.Pos) {
	err := s.cp.Save(ts, positions)
	if err != nil {
		log.Fatalf("[write save point]%d[positions]%v[error]%v", ts, positions, err)
	}

	positionGauge.Set(float64(ts))
}

func (s *Syncer) sync(executor executor.Executor, jobChan chan *job) {
	s.wg.Add(1)
	defer s.wg.Done()

	idx := 0
	count := s.cfg.TxnBatch
	sqls := make([]string, 0, count)
	args := make([][]interface{}, 0, count)
	commitTSs := make([]int64, 0, count)
	lastSyncTime := time.Now()
	tpCnt := make(map[pb.MutationType]int)

	clearF := func() {
		for i := 0; i < idx; i++ {
			s.jobWg.Done()
		}

		idx = 0
		sqls = sqls[0:0]
		args = args[0:0]
		commitTSs = commitTSs[0:0]
		lastSyncTime = time.Now()
		for tpName, v := range tpCnt {
			s.addDMLCount(tpName, v)
			tpCnt[tpName] = 0
		}
	}

	var err error
	for {
		select {
		case job, ok := <-jobChan:
			if !ok {
				return
			}
			idx++

			if job.binlogTp == translator.DDL {
				// compute txn duration
				err = execute(executor, []string{job.sql}, [][]interface{}{job.args}, []int64{job.commitTS}, true)
				if err != nil {
					if !ignoreDDLError(err) {
						log.Fatalf(errors.ErrorStack(err))
					} else {
						log.Warnf("[ignore ddl error][sql]%s[args]%v[error]%v", job.sql, job.args, err)
					}
				}
				s.addDDLCount()
				clearF()
			} else if !job.isCompleteBinlog {
				sqls = append(sqls, job.sql)
				args = append(args, job.args)
				commitTSs = append(commitTSs, job.commitTS)
				tpCnt[job.mutationTp]++
			}

			if (!s.cfg.DisableDispatch && idx >= count) || job.isCompleteBinlog {
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

func (s *Syncer) run(b *binlogItem) error {
	s.wg.Add(1)
	defer func() {
		closeJobChans(s.jobCh)
		s.wg.Done()
	}()

	var err error

	s.genRegexMap()
	s.executors, err = createExecutors(s.cfg.DestDBType, s.cfg.To, s.cfg.WorkerCount)
	if err != nil {
		return errors.Trace(err)
	}

	s.translator, err = translator.New(s.cfg.DestDBType)
	if err != nil {
		return errors.Trace(err)
	}

	for i := 0; i < s.cfg.WorkerCount; i++ {
		go s.sync(s.executors[i], s.jobCh[i])
	}

	for {
		binlog := b.binlog
		commitTS := binlog.GetCommitTs()
		jobID := binlog.GetDdlJobId()

		if jobID == 0 {
			preWriteValue := binlog.GetPrewriteValue()
			preWrite := &pb.PrewriteValue{}
			err = preWrite.Unmarshal(preWriteValue)
			if err != nil {
				return errors.Errorf("prewrite %s unmarshal error %v", preWriteValue, err)
			}
			err = s.translateSqls(preWrite.GetMutations(), commitTS, b.pos, b.nodeID)
			if err != nil {
				return errors.Trace(err)
			}
			// send binlog boundary job for dml binlog, disdispatch also disables batch
			if s.cfg.DisableDispatch {
				s.addJob(newBinlogBoundaryJob(commitTS, b.pos, b.nodeID))
			}

		} else if jobID > 0 {
			schema, table, sql, err := s.handleDDL(b.job)
			if err != nil {
				return errors.Trace(err)
			}

			if s.skipSchemaAndTable(schema, table) {
				log.Infof("[skip ddl]db:%s table:%s, sql:%s, commit ts %d, pos %v", schema, table, sql, commitTS, b.pos)
			} else if sql != "" {
				sql, err = s.translator.GenDDLSQL(sql, schema)
				if err != nil {
					return errors.Trace(err)
				}

				log.Infof("[ddl][start]%s[commit ts]%v[pos]%v", sql, commitTS, b.pos)
				job := newDDLJob(sql, nil, "", commitTS, b.pos, b.nodeID)
				s.addJob(job)
				log.Infof("[ddl][end]%s[commit ts]%v[pos]%v", sql, commitTS, b.pos)
			}
		}

		select {
		case <-s.ctx.Done():
			return nil
		case b = <-s.input:
		}
	}
}

func (s *Syncer) translateSqls(mutations []pb.TableMutation, commitTS int64, pos pb.Pos, nodeID string) error {
	for _, mutation := range mutations {

		table, ok := s.schema.TableByID(mutation.GetTableId())
		if !ok {
			continue
		}

		schemaName, tableName, ok := s.schema.SchemaAndTableName(mutation.GetTableId())
		if !ok {
			continue
		}

		if s.skipSchemaAndTable(schemaName, tableName) {
			log.Debugf("[skip dml]db:%s table:%s", schemaName, tableName)
			continue
		}

		var (
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
			sqls[pb.MutationType_Insert], keys[pb.MutationType_Insert], args[pb.MutationType_Insert], err = s.translator.GenInsertSQLs(schemaName, table, mutation.GetInsertedRows())
			if err != nil {
				return errors.Errorf("gen insert sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}
			offsets[pb.MutationType_Insert] = 0
		}

		if len(mutation.GetUpdatedRows()) > 0 {
			// safemode is only work for mysql
			if s.cfg.SafeMode && s.cfg.DestDBType == "mysql" {
				sqls[pb.MutationType_Update], keys[pb.MutationType_Update], args[pb.MutationType_Update], err = s.translator.GenUpdateSQLsSafeMode(schemaName, table, mutation.GetUpdatedRows())
			} else {
				sqls[pb.MutationType_Update], keys[pb.MutationType_Update], args[pb.MutationType_Update], err = s.translator.GenUpdateSQLs(schemaName, table, mutation.GetUpdatedRows())
			}

			if err != nil {
				return errors.Errorf("gen update sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}
			offsets[pb.MutationType_Update] = 0
		}

		if len(mutation.GetDeletedRows()) > 0 {
			sqls[pb.MutationType_DeleteRow], keys[pb.MutationType_DeleteRow], args[pb.MutationType_DeleteRow], err = s.translator.GenDeleteSQLs(schemaName, table, mutation.GetDeletedRows())
			if err != nil {
				return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}
			offsets[pb.MutationType_DeleteRow] = 0
		}

		for _, dmlType := range sequences {
			if offsets[dmlType] >= len(sqls[dmlType]) {
				return errors.Errorf("gen sqls failed: sequence %v execution %s sqls %v", sequences, dmlType, sqls[dmlType])
			}

			// update is split to delete and insert
			if dmlType == pb.MutationType_Update && s.cfg.SafeMode && s.cfg.DestDBType == "mysql" {
				err = s.commitJob(pb.MutationType_DeleteRow, sqls[dmlType][offsets[dmlType]], args[dmlType][offsets[dmlType]], keys[dmlType][offsets[dmlType]], commitTS, pos, nodeID)
				if err != nil {
					return errors.Trace(err)
				}

				err = s.commitJob(pb.MutationType_Insert, sqls[dmlType][offsets[dmlType]+1], args[dmlType][offsets[dmlType]+1], keys[dmlType][offsets[dmlType]+1], commitTS, pos, nodeID)
				if err != nil {
					return errors.Trace(err)
				}
				offsets[dmlType] = offsets[dmlType] + 2
			} else {
				err = s.commitJob(dmlType, sqls[dmlType][offsets[dmlType]], args[dmlType][offsets[dmlType]], keys[dmlType][offsets[dmlType]], commitTS, pos, nodeID)
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
	}
}

// Close closes syncer.
func (s *Syncer) Close() {
	s.cancel()
	s.wg.Wait()
	closeExecutors(s.executors...)
}
