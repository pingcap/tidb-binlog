package drainer

import (
	"database/sql"
	"regexp"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/juju/errors"
	"github.com/ngaut/log"
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

// Executor converts tidb binlog to the specified DB sqls, and sync it to target DB
type Executor struct {
	schema *Schema
	meta   Meta

	cfg *ExecutorConfig

	translator translator.SQLTranslator

	wg sync.WaitGroup

	input chan *binlogItem
	jobWg sync.WaitGroup
	jobCh []chan *job

	toDBs []*sql.DB

	poss         map[string]pb.Pos
	initCommitTS int64

	ignoreSchemaNames map[string]struct{}

	ctx    context.Context
	cancel context.CancelFunc

	reMap map[string]*regexp.Regexp
}

// NewExecutor returns a Drainer instance
func NewExecutor(ctx context.Context, meta Meta, cfg *ExecutorConfig) (*Executor, error) {
	executor := new(Executor)
	executor.cfg = cfg
	executor.ignoreSchemaNames = formatIgnoreSchemas(cfg.IgnoreSchemas)
	executor.meta = meta
	executor.input = make(chan *binlogItem, 1024*cfg.WorkerCount)
	executor.jobCh = newJobChans(cfg.WorkerCount)
	executor.reMap = make(map[string]*regexp.Regexp)
	executor.ctx, executor.cancel = context.WithCancel(ctx)
	executor.initCommitTS, executor.poss = meta.Pos()

	return executor, nil
}

func newJobChans(count int) []chan *job {
	jobCh := make([]chan *job, 0, count)
	for i := 0; i < count; i++ {
		jobCh = append(jobCh, make(chan *job, 1024))
	}

	return jobCh
}

// Start starts to sync.
func (d *Executor) Start(jobs []*model.Job) error {
	// prepare schema for work
	b, err := d.prepare(jobs)
	if err != nil || b == nil {
		return errors.Trace(err)
	}

	err = d.run(b)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// the binlog maybe not complete before the initCommitTS, so we should ignore them.
// at the same time, we try to find the latest schema version before the initCommitTS to reconstruct local schemas.
func (d *Executor) prepare(jobs []*model.Job) (*binlogItem, error) {
	var latestSchemaVersion int64
	var b *binlogItem
	var err error

	for {
		select {
		case <-d.ctx.Done():
			return nil, nil
		case b = <-d.input:
		}

		binlog := b.binlog
		commitTS := binlog.GetCommitTs()
		jobID := binlog.GetDdlJobId()
		if commitTS < d.initCommitTS {
			if jobID > 0 {
				latestSchemaVersion = b.job.BinlogInfo.SchemaVersion
			}
			continue
		}

		// if don't meet ddl, we need to set lasteSchemaVersion to
		// 1. the current DML's schemaVerion
		// 2. the version that less than current DDL's version
		if latestSchemaVersion == 0 {
			if jobID == 0 {
				preWriteValue := binlog.GetPrewriteValue()
				preWrite := &pb.PrewriteValue{}
				err = preWrite.Unmarshal(preWriteValue)
				if err != nil {
					return nil, errors.Errorf("prewrite %s unmarshal error %v", preWriteValue, err)
				}
				latestSchemaVersion = preWrite.GetSchemaVersion()
			} else {
				// make the latestSchemaVersion less than the current ddl
				latestSchemaVersion = b.job.BinlogInfo.SchemaVersion - 1
			}
		}

		// find all ddl job that need to reconstruct local schemas
		var exceptedJobs []*model.Job
		for _, job := range jobs {
			if job.BinlogInfo.SchemaVersion <= latestSchemaVersion {
				exceptedJobs = append(exceptedJobs, job)
			}
		}

		d.schema, err = NewSchema(exceptedJobs, d.ignoreSchemaNames)
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
func (d *Executor) handleDDL(job *model.Job) (string, string, string, error) {
	if job.State == model.JobCancelled {
		return "", "", "", nil
	}

	sql := job.Query
	if sql == "" {
		return "", "", "", errors.Errorf("[ddl job sql miss]%+v", job)
	}

	switch job.Type {
	case model.ActionCreateSchema:
		// get the DBInfo from job rawArgs
		schema := job.BinlogInfo.DBInfo
		if filterIgnoreSchema(schema, d.ignoreSchemaNames) {
			d.schema.AddIgnoreSchema(schema)
			return "", "", "", nil
		}

		err := d.schema.CreateSchema(schema)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.L, "", sql, nil

	case model.ActionDropSchema:
		_, ok := d.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			d.schema.DropIgnoreSchema(job.SchemaID)
			return "", "", "", nil
		}

		schemaName, err := d.schema.DropSchema(job.SchemaID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schemaName, "", sql, nil

	case model.ActionRenameTable:
		// ignore schema doesn't support reanme ddl
		_, ok := d.schema.SchemaByTableID(job.TableID)
		if !ok {
			return "", "", "", errors.NotFoundf("table(%d) or it's schema", job.TableID)
		}
		_, ok = d.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", "", errors.Errorf("ignore schema %d doesn't support rename ddl sql %s", job.SchemaID, sql)
		}
		// first drop the table
		_, err := d.schema.DropTable(job.TableID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}
		// create table
		table := job.BinlogInfo.TableInfo
		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err = d.schema.CreateTable(schema, table)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.L, table.Name.L, sql, nil

	case model.ActionCreateTable:
		table := job.BinlogInfo.TableInfo
		if table == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}

		_, ok := d.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", "", nil
		}

		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err := d.schema.CreateTable(schema, table)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.L, table.Name.L, sql, nil

	case model.ActionDropTable:
		_, ok := d.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", "", nil
		}

		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		tableName, err := d.schema.DropTable(job.TableID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.L, tableName, sql, nil

	case model.ActionTruncateTable:
		_, ok := d.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", "", nil
		}

		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		_, err := d.schema.DropTable(job.TableID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		table := job.BinlogInfo.TableInfo
		if table == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}

		err = d.schema.CreateTable(schema, table)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.L, table.Name.L, sql, nil

	default:
		tbInfo := job.BinlogInfo.TableInfo
		if tbInfo == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}

		_, ok := d.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", "", nil
		}

		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err := d.schema.ReplaceTable(tbInfo)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.L, tbInfo.Name.L, sql, nil
	}
}

func (d *Executor) addCount(tp translator.OpType, nums int) {
	switch tp {
	case translator.Insert:
		eventCounter.WithLabelValues("Insert").Add(float64(nums))
	case translator.Update:
		eventCounter.WithLabelValues("Update").Add(float64(nums))
	case translator.Del:
		eventCounter.WithLabelValues("Delete").Add(float64(nums))
	case translator.DDL:
		eventCounter.WithLabelValues("DDL").Add(float64(nums))
	}
}

func (d *Executor) checkWait(job *job) bool {
	if job.tp == translator.DDL {
		return true
	}
	if d.meta.Check() {
		return true
	}
	return false
}

type job struct {
	tp       translator.OpType
	sql      string
	args     []interface{}
	key      string
	commitTS int64
	pos      pb.Pos
	nodeID   string
}

func newJob(tp translator.OpType, sql string, args []interface{}, key string, commitTS int64, pos pb.Pos, nodeID string) *job {
	return &job{tp: tp, sql: sql, args: args, key: key, commitTS: commitTS, pos: pos, nodeID: nodeID}
}

func (d *Executor) addJob(job *job) {
	// make all DMLs be executed before DDL
	if job.tp == translator.DDL {
		d.jobWg.Wait()
	}

	d.jobWg.Add(1)
	idx := int(genHashKey(job.key)) % d.cfg.WorkerCount
	d.jobCh[idx] <- job

	if pos, ok := d.poss[job.nodeID]; !ok || pos.Suffix < job.pos.Suffix {
		d.poss[job.nodeID] = job.pos
	}

	wait := d.checkWait(job)
	if wait {
		d.jobWg.Wait()
		d.savePoint(job.commitTS, d.poss)
	}
}

func (d *Executor) savePoint(ts int64, poss map[string]pb.Pos) {
	err := d.meta.Save(ts, poss)
	if err != nil {
		log.Fatalf("[write save point]%d[error]%v", ts, err)
	}

	positionGauge.Set(float64(ts))
}

func (d *Executor) sync(db *sql.DB, jobChan chan *job) {
	d.wg.Add(1)
	defer d.wg.Done()

	idx := 0
	count := d.cfg.TxnBatch
	sqls := make([]string, 0, count)
	args := make([][]interface{}, 0, count)
	lastSyncTime := time.Now()
	tpCnt := make(map[translator.OpType]int)

	clearF := func() {
		for i := 0; i < idx; i++ {
			d.jobWg.Done()
		}

		idx = 0
		sqls = sqls[0:0]
		args = args[0:0]
		lastSyncTime = time.Now()
		for tpName, v := range tpCnt {
			d.addCount(tpName, v)
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

			if job.tp == translator.DDL {
				err = executeSQLs(db, []string{job.sql}, [][]interface{}{job.args}, true)
				if err != nil {
					if !ignoreDDLError(err) {
						log.Fatalf(errors.ErrorStack(err))
					} else {
						log.Warnf("[ignore ddl error][sql]%s[args]%v[error]%v", job.sql, job.args, err)
					}
				}
				tpCnt[job.tp]++
				clearF()
			} else {
				sqls = append(sqls, job.sql)
				args = append(args, job.args)
				tpCnt[job.tp]++
			}

			if idx >= count {
				err = executeSQLs(db, sqls, args, false)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}
				clearF()
			}

		default:
			now := time.Now()
			if now.Sub(lastSyncTime) >= maxExecutionWaitTime {
				err = executeSQLs(db, sqls, args, false)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}
				clearF()
			}

			time.Sleep(executionWaitTime)
		}
	}
}

func (d *Executor) run(b *binlogItem) error {
	var err error

	d.genRegexMap()
	d.toDBs, err = createDBs(d.cfg.DestDBType, d.cfg.To, d.cfg.WorkerCount)
	if err != nil {
		return errors.Trace(err)
	}

	d.translator, err = translator.New(d.cfg.DestDBType)
	if err != nil {
		return errors.Trace(err)
	}

	for i := 0; i < d.cfg.WorkerCount; i++ {
		go d.sync(d.toDBs[i], d.jobCh[i])
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
			err = d.translateSqls(preWrite.GetMutations(), commitTS, b.pos, b.nodeID)
			if err != nil {
				return errors.Trace(err)
			}
		} else if jobID > 0 {
			schema, table, sql, err := d.handleDDL(b.job)
			if err != nil {
				return errors.Trace(err)
			}

			if d.skipDDL(schema, table) {
				log.Debugf("[skip ddl]db:%s table:%s, sql:%s, commit ts %d, pos %v", schema, table, sql, commitTS, b.pos)
				continue
			}

			if sql != "" {
				sql, err = d.translator.GenDDLSQL(sql, schema)
				if err != nil {
					return errors.Trace(err)
				}

				log.Infof("[ddl][start]%s[commit ts]%v[pos]%v", sql, commitTS, b.pos)
				job := newJob(translator.DDL, sql, nil, "", commitTS, b.pos, b.nodeID)
				d.addJob(job)
				log.Infof("[ddl][end]%s[commit ts]%v[pos]%v", sql, commitTS, b.pos)
			}
		}

		select {
		case <-d.ctx.Done():
			return nil
		case b = <-d.input:
		}
	}
}

func (d *Executor) translateSqls(mutations []pb.TableMutation, commitTS int64, pos pb.Pos, nodeID string) error {
	for _, mutation := range mutations {

		table, ok := d.schema.TableByID(mutation.GetTableId())
		if !ok {
			continue
		}

		schemaName, tableName, ok := d.schema.SchemaAndTableName(mutation.GetTableId())
		if !ok {
			continue
		}

		if d.skipDML(schemaName, tableName) {
			log.Debugf("[skip dml]db:%s table:%s", schemaName, tableName)
			continue
		}

		var (
			err error

			// the restored sqls, 0 => insert, 1 => update, 2 => deleteByIds, 3 => deleteByPks, 4 => deleteByRows
			sqls = make([][]string, 5)

			// the dispatch keys
			keys = make([][]string, 5)

			// the restored sqls's args, ditto
			args = make([][][]interface{}, 5)

			// the offset of specified type sql
			offsets = make([]int, 5)

			// the binlog dml sort
			sequences = mutation.GetSequence()

			// sql opType
			tps = []translator.OpType{translator.Insert, translator.Update, translator.Del, translator.Del, translator.Del}
		)

		if len(mutation.GetInsertedRows()) > 0 {
			sqls[0], keys[0], args[0], err = d.translator.GenInsertSQLs(schemaName, table, mutation.GetInsertedRows())
			if err != nil {
				return errors.Errorf("gen insert sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}
		}

		if len(mutation.GetUpdatedRows()) > 0 {
			sqls[1], keys[1], args[1], err = d.translator.GenUpdateSQLs(schemaName, table, mutation.GetUpdatedRows())
			if err != nil {
				return errors.Errorf("gen update sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}
		}

		if len(mutation.GetDeletedIds()) > 0 {
			sqls[2], keys[2], args[2], err = d.translator.GenDeleteSQLsByID(schemaName, table, mutation.GetDeletedIds())
			if err != nil {
				return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}
		}

		if len(mutation.GetDeletedPks()) > 0 {
			sqls[3], keys[3], args[3], err = d.translator.GenDeleteSQLs(schemaName, table, translator.DelByPK, mutation.GetDeletedPks())
			if err != nil {
				return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}
		}

		if len(mutation.GetDeletedRows()) > 0 {
			sqls[4], keys[4], args[4], err = d.translator.GenDeleteSQLs(schemaName, table, translator.DelByCol, mutation.GetDeletedRows())
			if err != nil {
				return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}
		}

		for _, dmlType := range sequences {
			index := int32(dmlType)
			if offsets[index] >= len(sqls[index]) {
				return errors.Errorf("gen sqls failed: sequence %v execution %s sqls %v", sequences, dmlType, sqls[index])
			}

			job := newJob(tps[index], sqls[index][offsets[index]], args[index][offsets[index]], keys[index][offsets[index]], commitTS, pos, nodeID)
			d.addJob(job)
			offsets[index] = offsets[index] + 1
		}

		// Compatible with the old format that don't have sequence, will be remove in the futhure
		for i := 0; i < 5; i++ {
			for j := offsets[i]; j < len(sqls[i]); j++ {
				job := newJob(tps[i], sqls[i][j], args[i][j], keys[i][j], commitTS, pos, nodeID)
				d.addJob(job)
			}
		}
	}

	return nil
}

// AddToExectorChan adds binlogItem to the Executor's input channel
func (d *Executor) AddToExectorChan(b *binlogItem) {
	d.input <- b
}

// Close closes syncer.
func (d *Executor) Close() {
	d.cancel()
	d.wg.Wait()
	closeDBs(d.toDBs...)
}
