package drainer

import (
	"database/sql"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/siddontang/go/sync2"
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

	store         kv.Storage
	toDB          *sql.DB
	cisternClient pb.CisternClient

	start    time.Time
	lastTime time.Time

	ddlCount    sync2.AtomicInt64
	insertCount sync2.AtomicInt64
	updateCount sync2.AtomicInt64
	deleteCount sync2.AtomicInt64
	lastCount   sync2.AtomicInt64
	count       sync2.AtomicInt64

	ctx    context.Context
	cancel context.CancelFunc
}

// NewDrainer returns a Drainer instance
func NewDrainer(cfg *Config, store kv.Storage, cisternClient pb.CisternClient) (*Drainer, error) {
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return nil, err
	}

	drainer := new(Drainer)
	drainer.cfg = cfg
	drainer.store = store
	drainer.cisternClient = cisternClient
	drainer.meta = NewLocalMeta(path.Join(cfg.DataDir, "savePoint"))
	drainer.input = make(chan []byte, 1024)
	drainer.ctx, drainer.cancel = context.WithCancel(context.Background())

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

	// sync the schema at meta.Pos
	d.schema, err = NewSchema(d.store, uint64(d.meta.Pos()))
	if err != nil {
		return errors.Trace(err)
	}

	err = d.run()
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (d *Drainer) savePoint(ts int64) {
	err := d.meta.Save(ts)
	if err != nil {
		log.Fatalf("[write save point]%d[error]%v", ts, err)
	}
}

func (d *Drainer) getHistoryJob(id int64) (*model.Job, error) {
	version, err := d.store.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}

	snapshot, err := d.store.GetSnapshot(version)
	if err != nil {
		return nil, errors.Trace(err)
	}

	snapMeta := meta.NewSnapshotMeta(snapshot)
	job, err := snapMeta.GetHistoryDDLJob(id)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return job, err
}

func (d *Drainer) getSnapShotTable(snapshotVer uint64, schemaID int64, tableID int64) (*model.TableInfo, error) {
	version := kv.NewVersion(snapshotVer)
	snapshot, err := d.store.GetSnapshot(version)
	if err != nil {
		return nil, errors.Trace(err)
	}

	snapMeta := meta.NewSnapshotMeta(snapshot)

	table, err := snapMeta.GetTable(schemaID, tableID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return table, nil
}

// while appears ddl, we would wait ddl completed.
// According to tidb ddl source code, the ways that we will change local schema as blew:
// 1 we can get the newest schema info from job if the ddl type is ActionCreateSchema/ActionCreateTable.
// 2 we can change the local schema info if the ddl type is ActionDropSchema/ActionDropTable.
// 3 we can get the schema info from the job whose state is StateReorganization, then do some small adjustments
//   if the ddl type is ActionAddColumn/ActionDropColumn/ActionAddIndex
// 4 we can just do samll adjustments if the ddl type is ActionDropIndex
// 5 we don't need to change the local schema info if the ddl type is ActionAddForeignKey/ActionDropForeignKey
func (d *Drainer) handleDDL(id int64, sql string) (string, string, bool, error) {
	job, err := d.getHistoryJob(id)
	if err != nil {
		return "", "", false, errors.Trace(err)
	}

	tmpDelay := retryTimeout
	for job == nil {
		select {
		case <-d.ctx.Done():
			return "", "", false, errors.Trace(err)
		case <-time.After(tmpDelay):
			tmpDelay *= 2
			if tmpDelay > maxWaitGetJobTime {
				tmpDelay = maxWaitGetJobTime
			}

			job, err = d.getHistoryJob(id)
			if err != nil {
				return "", "", false, errors.Trace(err)
			}
		}
	}

	if job.State == model.JobCancelled {
		return "", "", false, nil
	}

	switch job.Type {
	case model.ActionCreateSchema:
		// get the DBInfo from job rawArgs
		schema := &model.DBInfo{}
		if err := job.DecodeArgs(schema); err != nil {
			return "", "", true, errors.Trace(err)
		}

		err = d.schema.CreateSchema(schema)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}

		return schema.Name.L, sql, true, nil

	case model.ActionDropSchema:
		schemaName, err := d.schema.DropSchema(job.SchemaID)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}

		return schemaName, sql, true, nil

	case model.ActionCreateTable:
		// get the TableInfo from job rawArgs
		table := &model.TableInfo{}
		if err := job.DecodeArgs(table); err != nil {
			return "", "", true, errors.Trace(err)
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

	case model.ActionAddColumn:
		table, err := d.getSnapShotTable(job.SnapshotVer, job.SchemaID, job.TableID)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}
		if table == nil {
			return "", "", true, errors.NotFoundf("table %d", job.TableID)
		}

		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", true, errors.NotFoundf("schema %d", job.SchemaID)
		}

		err = adjustColumn(table, job)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}

		err = d.schema.ReplaceTable(table)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}

		return schema.Name.L, sql, true, nil

	case model.ActionDropColumn:
		table, err := d.getSnapShotTable(job.SnapshotVer, job.SchemaID, job.TableID)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}
		if table == nil {
			return "", "", true, errors.NotFoundf("table %d", job.TableID)
		}

		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", true, errors.NotFoundf("schema %d", job.SchemaID)
		}

		var colName model.CIStr
		err = job.DecodeArgs(&colName)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}

		newColumns := make([]*model.ColumnInfo, 0, len(table.Columns))
		for _, col := range table.Columns {
			if col.Name.L != colName.L {
				newColumns = append(newColumns, col)
			}
		}
		table.Columns = newColumns

		err = d.schema.ReplaceTable(table)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}

		return schema.Name.L, sql, true, nil

	case model.ActionAddIndex:
		table, err := d.getSnapShotTable(job.SnapshotVer, job.SchemaID, job.TableID)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}
		if table == nil {
			return "", "", true, errors.NotFoundf("table %d", job.TableID)
		}

		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", true, errors.NotFoundf("schema %d", job.SchemaID)
		}

		err = adjustTableIndex(table, job, true)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}

		err = d.schema.ReplaceTable(table)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}

		return schema.Name.L, sql, true, nil

	case model.ActionDropIndex:
		table, ok := d.schema.TableByID(job.TableID)
		if !ok {
			return "", "", true, errors.NotFoundf("table %d", job.TableID)
		}

		schema, ok := d.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", true, errors.NotFoundf("schema %d", job.SchemaID)
		}

		err = adjustTableIndex(table, job, false)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}

		err = d.schema.ReplaceTable(table)
		if err != nil {
			return "", "", true, errors.Trace(err)
		}

		return schema.Name.L, sql, true, nil

	case model.ActionAddForeignKey, model.ActionDropForeignKey:
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
		d.insertCount.Add(1)
	case translator.Update:
		d.updateCount.Add(1)
	case translator.Del:
		d.deleteCount.Add(1)
	case translator.DDL:
		d.ddlCount.Add(1)
	}

	d.count.Add(1)
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

	d.start = time.Now()
	d.lastTime = d.start

	go d.printStatus()
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
				log.Errorf("[exec sqls error][ignore ddl error][sql]%v[args]%v[error]%v", b.sqls, b.args, err)
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
					log.Errorf("[exec ddl error][ignore ddl error][sql]%v[args]%v[error]%v", b.sqls, b.args, err)
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

func (d *Drainer) printStatus() {
	d.wg.Add(1)
	defer d.wg.Done()

	timer := time.NewTicker(statusTime)
	defer timer.Stop()

	for {
		select {
		case <-d.ctx.Done():
			return
		case <-timer.C:
			now := time.Now()
			seconds := now.Unix() - d.lastTime.Unix()
			totalSeconds := now.Unix() - d.start.Unix()
			last := d.lastCount.Get()
			total := d.count.Get()

			tps, totalTps := int64(0), int64(0)
			if seconds > 0 {
				tps = (total - last) / seconds
				totalTps = total / totalSeconds
			}

			log.Infof("[syncer]total events = %d, insert = %d, update = %d, delete = %d, total tps = %d, recent tps = %d, %s.",
				total, d.insertCount.Get(), d.updateCount.Get(), d.deleteCount.Get(), totalTps, tps, d.meta)

			d.lastCount.Set(total)
			d.lastTime = time.Now()
		}
	}
}

func (d *Drainer) inputStreaming() {
	d.wg.Add(1)
	defer d.wg.Done()

	var err error
	var count int
	var resp *pb.DumpBinlogResp

	nextRequestTS := d.meta.Pos()
	tmpDelay := time.Duration(0)

	for {

		select {
		case <-d.ctx.Done():
			return
		case <-time.After(tmpDelay):
			if count > 0 {
				d.input <- resp.Payloads[len(resp.Payloads)-count]
				count--
				continue
			}

			req := &pb.DumpBinlogReq{BeginCommitTS: nextRequestTS, Limit: int32(d.cfg.RequestCount)}
			resp, err = d.cisternClient.DumpBinlog(d.ctx, req)
			if err != nil {
				log.Warning(err)
			}
			if err == nil && resp.Errmsg != "" {
				log.Warning(resp.Errmsg)
			}

			if resp == nil || len(resp.Payloads) == 0 {
				tmpDelay = retryTimeout
			} else {
				count = len(resp.Payloads)
				nextRequestTS = resp.EndCommitTS
				tmpDelay = 0
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

	if d.store != nil {
		d.store.Close()
	}

	if d.toDB != nil {
		closeDB(d.toDB)
	}
}
