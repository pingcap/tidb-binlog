package drainer

import (
	"database/sql"
	"io"
	"os"
	"path"
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

	wg sync.WaitGroup

	input chan []byte

	jLock sync.RWMutex
	jobs  map[int64]*model.Job

	toDB          *sql.DB
	cisternClient pb.CisternClient

	ignoreSchemaNames map[string]struct{}

	metrics *metricClient

	ctx    context.Context
	cancel context.CancelFunc
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
	drainer.input = make(chan []byte, 1024)
	drainer.jobs = make(map[int64]*model.Job)
	drainer.ctx, drainer.cancel = context.WithCancel(context.Background())

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
// the second value[string]: the sql that is corresponding to the job
// the third value[error]: the handleDDL execution's err
func (d *Drainer) handleDDL(id int64) (string, string, error) {
	d.jLock.RLock()
	job, ok := d.jobs[id]
	d.jLock.RUnlock()
	if !ok {
		return "", "", errors.Errorf("[ddl job miss]%v", id)
	}

	if job.State == model.JobCancelled {
		return "", "", nil
	}

	sql := job.Query
	if sql == "" {
		return "", "", errors.Errorf("[ddl job sql miss]%v", id)
	}

	var err error
	switch job.Type {
	case model.ActionCreateSchema:
		// get the DBInfo from job rawArgs
		schema := job.BinlogInfo.DBInfo
		if filterIgnoreSchema(schema, d.ignoreSchemaNames) {
			d.schema.AddIgnoreSchema(schema)
			return "", "", nil
		}

		err = d.schema.CreateSchema(schema)
		if err != nil {
			return "", "", errors.Trace(err)
		}

		return schema.Name.L, sql, nil

	case model.ActionDropSchema:
		_, ok := d.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			d.schema.DropIgnoreSchema(job.SchemaID)
			return "", "", nil
		}

		schemaName, err := d.schema.DropSchema(job.SchemaID)
		if err != nil {
			return "", "", errors.Trace(err)
		}

		return schemaName, sql, nil

	case model.ActionCreateTable:
		// get the TableInfo from job rawArgs
		table := job.BinlogInfo.TableInfo
		if table == nil {
			return "", "", errors.NotFoundf("table %d", job.TableID)
		}

		_, ok := d.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", nil
		}

		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err = d.schema.CreateTable(schema, table)
		if err != nil {
			return "", "", errors.Trace(err)
		}

		return schema.Name.L, sql, nil

	case model.ActionDropTable:
		_, ok := d.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", nil
		}

		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		_, err := d.schema.DropTable(job.TableID)
		if err != nil {
			return "", "", errors.Trace(err)
		}

		return schema.Name.L, sql, nil

	case model.ActionTruncateTable:
		_, ok := d.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", nil
		}

		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		_, err := d.schema.DropTable(job.TableID)
		if err != nil {
			return "", "", errors.Trace(err)
		}

		table := job.BinlogInfo.TableInfo
		if table == nil {
			return "", "", errors.NotFoundf("table %d", job.TableID)
		}

		err = d.schema.CreateTable(schema, table)
		if err != nil {
			return "", "", errors.Trace(err)
		}

		return schema.Name.L, sql, nil

	default:
		tbInfo := job.BinlogInfo.TableInfo
		if tbInfo == nil {
			return "", "", errors.NotFoundf("table %d", job.TableID)
		}

		_, ok := d.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", nil
		}

		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err = d.schema.ReplaceTable(tbInfo)
		if err != nil {
			return "", "", errors.Trace(err)
		}

		return schema.Name.L, sql, nil
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

type batch struct {
	isDDL    bool
	sqls     []string
	args     [][]interface{}
	retry    bool
	commitTS int64
}

func newBatch(isDDL, retry bool, commitTS int64) *batch {
	return &batch{
		isDDL:    isDDL,
		retry:    retry,
		commitTS: commitTS,
	}
}

func (b *batch) addJob(sql string, arg []interface{}) {
	b.sqls = append(b.sqls, sql)
	b.args = append(b.args, arg)
}

func (b *batch) applyBatch(db *sql.DB) error {
	beginTime := time.Now()
	err := executeSQLs(db, b.sqls, b.args, b.retry)
	if err != nil {
		if !b.isDDL || !ignoreDDLError(err) {
			return errors.Trace(err)
		}

		log.Warnf("[ignore ddl error][sql]%v[args]%v[error]%v", b.sqls, b.args, err)
	}
	txnHistogram.Observe(time.Since(beginTime).Seconds())
	return nil
}

func (d *Drainer) run() error {
	d.wg.Add(1)
	defer d.wg.Done()

	var err error
	var rawBinlog []byte

	d.toDB, err = openDB(d.cfg.To.User, d.cfg.To.Password, d.cfg.To.Host, d.cfg.To.Port, d.cfg.DestDBType)
	if err != nil {
		return errors.Trace(err)
	}

	d.translator, err = translator.New(d.cfg.DestDBType)
	if err != nil {
		return errors.Trace(err)
	}

	go d.pushMetrics()
	go d.inputStreaming()

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

			b := newBatch(false, true, commitTS)
			err = d.translateSqls(preWrite.GetMutations(), b)
			if err != nil {
				return errors.Trace(err)
			}

			err = b.applyBatch(d.toDB)
			if err != nil {
				log.Errorf("[exec sqls error][dml error][sql]%v[args]%v[error]%v", b.sqls, b.args, err)
				return errors.Trace(err)
			}
			d.savePoint(commitTS)

		} else if jobID > 0 {
			schema, sql, err := d.handleDDL(jobID)
			if err != nil {
				return errors.Trace(err)
			}
			d.jLock.Lock()
			delete(d.jobs, jobID)
			d.jLock.Unlock()

			if sql != "" {
				sql, err = d.translator.GenDDLSQL(sql, schema)
				if err != nil {
					return errors.Trace(err)
				}

				log.Infof("[ddl][start]%s[pos]%v", sql, commitTS)

				d.addCount(translator.DDL, 1)

				b := newBatch(true, false, commitTS)
				b.addJob(sql, []interface{}{})

				err = b.applyBatch(d.toDB)
				if err != nil {
					log.Errorf("[exec ddl error][ddl error][sql]%v[args]%v[error]%v", b.sqls, b.args, err)
					return errors.Trace(err)
				}

				log.Infof("[ddl][end]%s[pos]%v", sql, commitTS)
				d.savePoint(commitTS)
			}
		}

		if d.cfg.EndCommitTS > 0 && commitTS >= d.cfg.EndCommitTS {
			log.Info("recovery complete!")
			return nil
		}
	}
}

func (d *Drainer) translateSqls(mutations []pb.TableMutation, b *batch) error {
	for _, mutation := range mutations {

		table, ok := d.schema.TableByID(mutation.GetTableId())
		if !ok {
			continue
		}

		schemaName, tableName, ok := d.schema.SchemaAndTableName(mutation.GetTableId())
		if !ok {
			continue
		}

		var (
			err error

			// the restored sqls, 0 => insert, 1 => update, 2 => deleteByIds, 3 => deleteByPks, 4 => deleteByRows
			sqls = make([][]string, 5)

			// the restored sqls's args, ditto
			args = make([][][]interface{}, 5)

			// the offset of specified type sql
			offsets = make([]int, 5)

			// the binlog dml sort
			sequences = mutation.GetSequence()
		)

		if len(mutation.GetInsertedRows()) > 0 {
			sqls[0], args[0], err = d.translator.GenInsertSQLs(schemaName, table, mutation.GetInsertedRows())
			if err != nil {
				return errors.Errorf("gen insert sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}

			d.addCount(translator.Insert, len(sqls[0]))
		}

		if len(mutation.GetUpdatedRows()) > 0 {
			sqls[1], args[1], err = d.translator.GenUpdateSQLs(schemaName, table, mutation.GetUpdatedRows())
			if err != nil {
				return errors.Errorf("gen update sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}

			d.addCount(translator.Update, len(sqls[1]))
		}

		if len(mutation.GetDeletedIds()) > 0 {
			sqls[2], args[2], err = d.translator.GenDeleteSQLsByID(schemaName, table, mutation.GetDeletedIds())
			if err != nil {
				return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}

			d.addCount(translator.Del, len(sqls[2]))
		}

		if len(mutation.GetDeletedPks()) > 0 {
			sqls[3], args[3], err = d.translator.GenDeleteSQLs(schemaName, table, translator.DelByPK, mutation.GetDeletedPks())
			if err != nil {
				return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}

			d.addCount(translator.Del, len(sqls[3]))
		}

		if len(mutation.GetDeletedRows()) > 0 {
			sqls[4], args[4], err = d.translator.GenDeleteSQLs(schemaName, table, translator.DelByCol, mutation.GetDeletedRows())
			if err != nil {
				return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}

			d.addCount(translator.Del, len(sqls[4]))
		}

		for _, dmlType := range sequences {
			index := int32(dmlType)
			if offsets[index] >= len(sqls[index]) {
				return errors.Errorf("gen sqls failed: sequence %v execution %s sqls %v", sequences, dmlType, sqls[index])
			}

			b.addJob(sqls[index][offsets[index]], args[index][offsets[index]])
			offsets[index] = offsets[index] + 1
		}

		// Compatible with the old format that don't have sequence, will be remove in the futhure
		for i := 0; i < 5; i++ {
			for j := offsets[i]; j < len(sqls[i]); j++ {
				b.addJob(sqls[i][j], args[i][j])
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

	if d.toDB != nil {
		closeDB(d.toDB)
	}
}
