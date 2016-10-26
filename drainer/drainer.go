package drainer

import (
	"database/sql"
	"fmt"
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
	retryTimeout      = 3 * time.Second
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
	d.schema, err = NewSchema(jobs, d.meta.Pos(), d.ignoreSchemaNames)
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
	tmpDelay := time.Duration(0)

	for {
		select {
		case <-d.ctx.Done():
			return nil, nil
		case <-time.After(tmpDelay):
			resp, err = d.cisternClient.DumpDDLJobs(d.ctx, req)
			if err != nil {
				log.Warningf("[can't get history job]%v", err)
				tmpDelay = retryTimeout
				continue
			}
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
}

// handleDDL has four return values,
// the first value[string]: the schema name
// the second value[string]: the sql that is corresponding to the job
// the third value[bool]: whether the job is need to execute
// the fourth value[error]: the handleDDL execution's err
func (d *Drainer) handleDDL(id int64, sql string) (string, string, bool, error) {
	job, ok := d.jobs[id]
	if !ok {
		return "", "", false, errors.Errorf("[ddl jon miss]%v", id)
	}

	if job.State == model.JobCancelled {
		return "", "", false, nil
	}

	var err error
	switch job.Type {
	case model.ActionCreateSchema:
		// get the DBInfo from job rawArgs
		schema := &model.DBInfo{}
		if err := job.DecodeArgs(nil, schema); err != nil {
			return "", "", true, errors.Trace(err)
		}

		if filterIgnoreSchema(schema, d.ignoreSchemaNames) {
			d.schema.AddIgnoreSchema(schema)
			return "", "", false, nil
		}

		err = d.schema.CreateSchema(schema)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}

		return schema.Name.L, sql, true, nil

	case model.ActionDropSchema:
		_, ok := d.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			d.schema.DropIgnoreSchema(job.SchemaID)
			return "", "", false, nil
		}

		schemaName, err := d.schema.DropSchema(job.SchemaID)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}

		return schemaName, sql, true, nil

	case model.ActionCreateTable:
		// get the TableInfo from job rawArgs
		table := &model.TableInfo{}
		if err := job.DecodeArgs(nil, table); err != nil {
			return "", "", true, errors.Trace(err)
		}
		if table == nil {
			return "", "", true, errors.NotFoundf("table %d", job.TableID)
		}

		_, ok := d.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", false, nil
		}

		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", true, errors.NotFoundf("schema %d", job.SchemaID)
		}

		err = d.schema.CreateTable(schema, table)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}

		return schema.Name.L, sql, true, nil

	case model.ActionDropTable:
		_, ok := d.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", false, nil
		}

		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", true, errors.NotFoundf("schema %d", job.SchemaID)
		}

		tableName, err := d.schema.DropTable(job.TableID)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}

		sql = fmt.Sprintf("drop table %s", tableName)

		return schema.Name.L, sql, true, nil

	case model.ActionAddColumn, model.ActionDropColumn, model.ActionAddIndex, model.ActionDropIndex:
		tbInfo := &model.TableInfo{}
		err := job.DecodeArgs(nil, tbInfo)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}
		if tbInfo == nil {
			return "", "", true, errors.NotFoundf("table %d", job.TableID)
		}

		_, ok := d.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", false, nil
		}

		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", true, errors.NotFoundf("schema %d", job.SchemaID)
		}

		err = d.schema.ReplaceTable(tbInfo)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}

		return schema.Name.L, sql, true, nil

	case model.ActionAddForeignKey, model.ActionDropForeignKey:
		_, ok := d.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", "", false, nil
		}

		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", true, errors.NotFoundf("schema %d", job.SchemaID)
		}

		return schema.Name.L, sql, true, nil

	default:
		return "", "", true, errors.Errorf("invalid ddl %v", job)
	}
}

func (d *Drainer) addCount(tp translator.OpType) {
	switch tp {
	case translator.Insert:
		eventCounter.WithLabelValues("Insert").Inc()
	case translator.Update:
		eventCounter.WithLabelValues("Update").Inc()
	case translator.Del:
		eventCounter.WithLabelValues("Delete").Inc()
	case translator.DDL:
		eventCounter.WithLabelValues("DDL").Inc()
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

func (b *batch) addJob(sql string, args []interface{}) {
	b.sqls = append(b.sqls, sql)
	b.args = append(b.args, args)
}

func (b *batch) applyBatch(db *sql.DB) error {
	err := executeSQLs(db, b.sqls, b.args, b.retry)
	if err != nil {
		if !b.isDDL || !ignoreDDLError(err) {
			return errors.Trace(err)
		}

		log.Warnf("[ignore ddl error][sql]%v[args]%v[error]%v", b.sqls, b.args, err)
	}
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

		switch binlog.GetTp() {
		case pb.BinlogType_Commit:
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

		case pb.BinlogType_PostDDL:
			sql := string(binlog.GetDdlQuery())
			jobID := binlog.GetDdlJobId()

			schema, sql, ok, err := d.handleDDL(jobID, sql)
			if err != nil {
				return errors.Trace(err)
			}
			delete(d.jobs, jobID)

			if ok {
				sql, err = d.translator.GenDDLSQL(sql, schema)
				if err != nil {
					return errors.Trace(err)
				}

				log.Infof("[ddl][start]%s[pos]%v", sql, commitTS)

				d.addCount(translator.DDL)

				b := newBatch(true, false, commitTS)
				b.addJob(sql, nil)

				err = b.applyBatch(d.toDB)
				if err != nil {
					log.Errorf("[exec ddl error][ddl error][sql]%v[args]%v[error]%v", b.sqls, b.args, err)
					return errors.Trace(err)
				}

				log.Infof("[ddl][end]%s[pos]%v", sql, commitTS)
				d.savePoint(commitTS)
			}
		}
	}
}

func (d *Drainer) translateSqls(mutations []pb.TableMutation, b *batch) error {
	// bound , offset and operations are used to parition the sql type (insert, update, del)
	// bound stores the insert, update, delete bound position info
	// offset is just a cursor for record the sql position
	var (
		sqls   []string
		args   [][]interface{}
		bound  []int
		offset int
	)
	var operations = []translator.OpType{translator.Insert, translator.Update, translator.Del}

	for _, mutation := range mutations {

		table, ok := d.schema.TableByID(mutation.GetTableId())
		if !ok {
			continue
		}

		schemaName, tableName, ok := d.schema.SchemaAndTableName(mutation.GetTableId())
		if !ok {
			continue
		}

		if len(mutation.GetInsertedRows()) > 0 {
			sql, arg, err := d.translator.GenInsertSQLs(schemaName, table, mutation.GetInsertedRows())
			if err != nil {
				return errors.Errorf("gen insert sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}

			sqls = append(sqls, sql...)
			args = append(args, arg...)
			offset += len(sqls)
		}
		bound = append(bound, offset)

		if len(mutation.GetUpdatedRows()) > 0 {
			sql, arg, err := d.translator.GenUpdateSQLs(schemaName, table, mutation.GetUpdatedRows())
			if err != nil {
				return errors.Errorf("gen update sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}

			sqls = append(sqls, sql...)
			args = append(args, arg...)
			offset += len(sqls)
		}
		bound = append(bound, offset)

		if len(mutation.GetDeletedIds()) > 0 {
			sql, arg, err := d.translator.GenDeleteSQLsByID(schemaName, table, mutation.GetDeletedIds())
			if err != nil {
				return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}

			sqls = append(sqls, sql...)
			args = append(args, arg...)
			offset += len(sqls)
		}

		if len(mutation.GetDeletedPks()) > 0 {
			sql, arg, err := d.translator.GenDeleteSQLs(schemaName, table, translator.DelByPK, mutation.GetDeletedPks())
			if err != nil {
				return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}

			sqls = append(sqls, sql...)
			args = append(args, arg...)
			offset += len(sqls)
		}

		if len(mutation.GetDeletedRows()) > 0 {
			sql, arg, err := d.translator.GenDeleteSQLs(schemaName, table, translator.DelByCol, mutation.GetDeletedRows())
			if err != nil {
				return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, schemaName, tableName)
			}

			sqls = append(sqls, sql...)
			args = append(args, arg...)
			offset += len(sqls)
		}
		bound = append(bound, offset)

		offset = 0
		for i := range sqls {
			for i >= bound[offset] {
				offset++
			}

			d.addCount(operations[offset])
			b.addJob(sqls[i], args[i])
		}

		sqls = sqls[:0]
		args = args[:0]
		bound = bound[:0]
		offset = 0
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
	if stream != nil {
		resp, err := stream.Recv()
		if err != nil {
			return 0, errors.Trace(err)
		}

		if resp.Ddljob != nil {
			job := &model.Job{}
			err = job.Decode(resp.Ddljob)
			if err != nil {
				return 0, errors.Trace(err)
			}
			d.jobs[job.ID] = job
		}

		d.input <- resp.Payload
		return resp.CommitTS, nil
	}

	return 0, nil
}

func (d *Drainer) inputStreaming() {
	d.wg.Add(1)
	defer d.wg.Done()

	var stream pb.Cistern_DumpBinlogClient

	nextRequestTS := d.meta.Pos()
	tmpDelay := time.Duration(0)

	for {

		select {
		case <-d.ctx.Done():
			return
		case <-time.After(tmpDelay):
			nextTs, err := d.receiveBinlog(stream)
			if err != nil {
				tmpDelay = retryTimeout
				stream = nil
				if errors.Cause(err) != io.EOF {
					log.Errorf("[stream]%v", err)
				}
				continue
			}

			if nextTs == 0 {
				req := &pb.DumpBinlogReq{BeginCommitTS: nextRequestTS}
				stream, err = d.cisternClient.DumpBinlog(d.ctx, req)
				if err != nil {
					stream = nil
					tmpDelay = retryTimeout
					log.Error(err)
					continue
				}
			} else {
				nextRequestTS = nextTs
				tmpDelay = time.Duration(0)
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
