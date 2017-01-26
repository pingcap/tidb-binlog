package drainer

import (
	"database/sql"
	"io"
	"os"
	"path"
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
	maxRetryCount = 100

	maxWaitGetJobTime = 5 * time.Minute
	retryWaitTime     = 3 * time.Second
	waitTime          = 10 * time.Millisecond
	maxWaitTime       = 3 * time.Second
	eventTimeout      = 3 * time.Second
	statusTime        = 30 * time.Second
)

// Drainer converts tidb binlog to the specified DB sqls, and sync it to target DB
type Drainer struct {
	sync.Mutex

	cfg *Config

	schema *Schema
	meta   Meta

	translator translator.SQLTranslator

	jobWg sync.WaitGroup
	wg    sync.WaitGroup

	input chan []byte

	jLock sync.RWMutex
	jobs  map[int64]*model.Job
	jobCh []chan *job

	toDBs         []*sql.DB
	cisternClient pb.CisternClient

	ignoreSchemaNames map[string]struct{}

	metrics *metricClient

	ctx    context.Context
	cancel context.CancelFunc

	reMap map[string]*regexp.Regexp
}

// NewDrainer returns a Drainer instance
func NewDrainer(cfg *Config, cisternClient pb.CisternClient) (*Drainer, error) {
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return nil, err
	}

	drainer := new(Drainer)
	drainer.cfg = cfg
	drainer.cisternClient = cisternClient
	drainer.ignoreSchemaNames = formatIgnoreSchemas(cfg.IgnoreSchemas)
	drainer.meta = NewLocalMeta(path.Join(cfg.DataDir, "savePoint"))
	drainer.input = make(chan []byte, 10240)
	drainer.jobs = make(map[int64]*model.Job)
	drainer.jobCh = newJobChans(cfg.WorkerCount)
	drainer.ctx, drainer.cancel = context.WithCancel(context.Background())
	drainer.reMap = make(map[string]*regexp.Regexp)

	var metrics *metricClient
	if cfg.MetricsAddr != "" && cfg.MetricsInterval != 0 {
		metrics = &metricClient{
			addr:     cfg.MetricsAddr,
			interval: cfg.MetricsInterval,
		}
	}

	drainer.metrics = metrics

	return drainer, nil
}

func newJobChans(count int) []chan *job {
	jobCh := make([]chan *job, 0, count)
	for i := 0; i < count; i++ {
		jobCh = append(jobCh, make(chan *job, 1000))
	}

	return jobCh
}

// Start starts to sync.
func (d *Drainer) Start() error {
	var err error
	if d.cfg.InitCommitTS == 0 {
		err = d.meta.Load()
	} else {
		err = d.meta.Save(d.cfg.InitCommitTS)
	}
	if err != nil {
		return errors.Trace(err)
	}

	jobs, err := d.getHistoryJob(d.meta.Pos())
	if err != nil {
		return errors.Trace(err)
	}

	// sync the schema at meta.Pos
	d.schema, err = NewSchema(jobs, d.ignoreSchemaNames)
	if err != nil {
		return errors.Trace(err)
	}

	err = d.run()
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (d *Drainer) getHistoryJob(ts int64) ([]*model.Job, error) {
	var resp *pb.DumpDDLJobsResp
	var jobs []*model.Job
	var err error

	req := &pb.DumpDDLJobsReq{BeginCommitTS: ts}

	for {
		resp, err = d.cisternClient.DumpDDLJobs(d.ctx, req)
		if err != nil {
			log.Warningf("[can't get history job]%v", err)
			select {
			case <-d.ctx.Done():
				return nil, nil
			case <-time.After(retryWaitTime):
			}
			continue
		}
		break
	}

	for _, Ddljob := range resp.Ddljobs {
		job := &model.Job{}
		err = job.Decode(Ddljob)
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

func (d *Drainer) savePoint(ts int64) {
	err := d.meta.Save(ts)
	if err != nil {
		log.Fatalf("[write save point]%d[error]%v", ts, err)
	}

	positionGauge.Set(float64(ts))
}

// handleDDL has four return values,
// the first value[string]: the schema name
// the second value[string]: the table name
// the third value[string]: the sql that is corresponding to the job
// the fourth value[error]: the handleDDL execution's err
func (d *Drainer) handleDDL(id int64) (string, string, string, error) {
	d.jLock.RLock()
	job, ok := d.jobs[id]
	d.jLock.RUnlock()
	if !ok {
		return "", "", "", errors.Errorf("[ddl job miss]%v", id)
	}

	if job.State == model.JobCancelled {
		return "", "", "", nil
	}

	sql := job.Query
	if sql == "" {
		return "", "", "", errors.Errorf("[ddl job sql miss]%v", id)
	}

	var err error
	switch job.Type {
	case model.ActionCreateSchema:
		// get the DBInfo from job rawArgs
		schema := job.BinlogInfo.DBInfo
		if filterIgnoreSchema(schema, d.ignoreSchemaNames) {
			d.schema.AddIgnoreSchema(schema)
			return "", "", "", nil
		}

		err = d.schema.CreateSchema(schema)
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

		err = d.schema.CreateTable(schema, table)
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

		err = d.schema.ReplaceTable(tbInfo)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		return schema.Name.L, tbInfo.Name.L, sql, nil
	}
}

func (d *Drainer) addCount(tp translator.OpType, nums int) {
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

func (d *Drainer) checkWait(job *job) bool {
	if job.tp == translator.DDL {
		return true
	}
	if d.meta.Check() {
		return true
	}
	return false
}

type job struct {
	tp    translator.OpType
	sql   string
	args  []interface{}
	key   string
	retry bool
	pos   int64
}

func newJob(tp translator.OpType, sql string, args []interface{}, key string, retry bool, pos int64) *job {
	return &job{tp: tp, sql: sql, args: args, key: key, retry: retry, pos: pos}
}

func (d *Drainer) addJob(job *job) {
	// make all DMLs be executed before DDL
	if job.tp == translator.DDL {
		d.jobWg.Wait()
	}

	d.jobWg.Add(1)
	idx := int(genHashKey(job.key)) % d.cfg.WorkerCount
	d.jobCh[idx] <- job

	wait := d.checkWait(job)
	if wait {
		d.jobWg.Wait()
		d.savePoint(job.pos)
	}
}

func (d *Drainer) sync(db *sql.DB, jobChan chan *job) {
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
				err = executeSQLs(db, []string{job.sql}, [][]interface{}{job.args}, false)
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
				err = executeSQLs(db, sqls, args, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}
				clearF()
			}

		default:
			now := time.Now()
			if now.Sub(lastSyncTime) >= maxWaitTime {
				err = executeSQLs(db, sqls, args, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}
				clearF()
			}

			time.Sleep(waitTime)
		}
	}
}

func (d *Drainer) run() error {
	d.wg.Add(1)
	defer d.wg.Done()

	var err error
	var rawBinlog []byte

	d.genRegexMap()
	d.toDBs, err = createDBs(d.cfg.DestDBType, d.cfg.To, d.cfg.WorkerCount)
	if err != nil {
		return errors.Trace(err)
	}

	d.translator, err = translator.New(d.cfg.DestDBType)
	if err != nil {
		return errors.Trace(err)
	}

	go d.pushMetrics()
	go d.inputStreaming()

	for i := 0; i < d.cfg.WorkerCount; i++ {
		go d.sync(d.toDBs[i], d.jobCh[i])
	}

	for {

		select {
		case <-d.ctx.Done():
			return nil
		case rawBinlog = <-d.input:
		}

		binlog := &pb.Binlog{}
		err := binlog.Unmarshal(rawBinlog)
		if err != nil {
			return errors.Errorf("binlog %v unmarshal error %v", rawBinlog, err)
		}

		commitTS := binlog.GetCommitTs()
		jobID := binlog.GetDdlJobId()

		if jobID == 0 {
			preWriteValue := binlog.GetPrewriteValue()
			preWrite := &pb.PrewriteValue{}
			err = preWrite.Unmarshal(preWriteValue)
			if err != nil {
				return errors.Errorf("prewrite %s unmarshal error %v", preWriteValue, err)
			}
			err = d.translateSqls(preWrite.GetMutations(), commitTS)
			if err != nil {
				return errors.Trace(err)
			}
		} else if jobID > 0 {
			schema, table, sql, err := d.handleDDL(jobID)
			if err != nil {
				return errors.Trace(err)
			}
			d.jLock.Lock()
			delete(d.jobs, jobID)
			d.jLock.Unlock()

			if d.skipDDL(schema, table) {
				log.Debugf("[skip ddl]db:%s table:%s, sql:%s, commitTS %d", schema, table, sql, commitTS)
				continue
			}

			if sql != "" {
				sql, err = d.translator.GenDDLSQL(sql, schema)
				if err != nil {
					return errors.Trace(err)
				}

				log.Infof("[ddl][start]%s[pos]%v", sql, commitTS)
				job := newJob(translator.DDL, sql, nil, "", false, commitTS)
				d.addJob(job)
				log.Infof("[ddl][end]%s[pos]%v", sql, commitTS)
			}
		}

		if d.cfg.EndCommitTS > 0 && commitTS >= d.cfg.EndCommitTS {
			log.Info("recovery complete!")
			return nil
		}
	}
}

func (d *Drainer) translateSqls(mutations []pb.TableMutation, pos int64) error {
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

			job := newJob(tps[index], sqls[index][offsets[index]], args[index][offsets[index]], keys[index][offsets[index]], true, pos)
			d.addJob(job)
			offsets[index] = offsets[index] + 1
		}

		// Compatible with the old format that don't have sequence, will be remove in the futhure
		for i := 0; i < 5; i++ {
			for j := offsets[i]; j < len(sqls[i]); j++ {
				job := newJob(tps[i], sqls[i][j], args[i][j], keys[i][j], true, pos)
				d.addJob(job)
			}
		}
	}

	return nil
}

func (d *Drainer) pushMetrics() {
	if d.metrics == nil {
		return
	}
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.metrics.Start(d.ctx)
	}()
}

func (d *Drainer) receiveBinlog(stream pb.Cistern_DumpBinlogClient) (int64, error) {
	var nextTs int64
	var err error
	var resp *pb.DumpBinlogResp

	for {
		resp, err = stream.Recv()
		if err != nil {
			break
		}

		if resp.Ddljob != nil {
			job := &model.Job{}
			err = job.Decode(resp.Ddljob)
			if err != nil {
				break
			}

			d.jLock.Lock()
			d.jobs[job.ID] = job
			d.jLock.Unlock()
		}

		nextTs = resp.CommitTS
		log.Debugf("next request commitTS %d, input channel length %d", nextTs, d.input)
		d.input <- resp.Payload
	}

	return nextTs, errors.Trace(err)
}

func (d *Drainer) inputStreaming() {
	d.wg.Add(1)
	defer d.wg.Done()

	var err error
	var stream pb.Cistern_DumpBinlogClient
	nextRequestTS := d.meta.Pos()

	for {
		select {
		case <-d.ctx.Done():
			return
		default:
			req := &pb.DumpBinlogReq{BeginCommitTS: nextRequestTS}
			stream, err = d.cisternClient.DumpBinlog(d.ctx, req)
			if err != nil {
				log.Warningf("[Get stream]%v", err)
				time.Sleep(retryWaitTime)
				continue
			}

			nextTs, err := d.receiveBinlog(stream)
			if nextTs != 0 {
				nextRequestTS = nextTs
			}
			if err != nil {
				if errors.Cause(err) != io.EOF {
					log.Warningf("[stream]%v", err)
				}
				time.Sleep(retryWaitTime)
				continue
			}
		}
	}
}

// Close closes syncer.
func (d *Drainer) Close() {
	d.Lock()
	defer d.Unlock()

	d.cancel()

	d.wg.Wait()

	closeDBs(d.toDBs...)
}
