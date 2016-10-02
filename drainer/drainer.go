package drainer

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/proto"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/siddontang/go/sync2"
)

var (
	maxRetryCount = 100

	maxWaitGetJobTime       = 5 * time.Minute
	retryTimeout            = 3 * time.Second
	waitTime                = 10 * time.Millisecond
	maxWaitTime             = 3 * time.Second
	eventTimeout            = 3 * time.Second
	statusTime              = 30 * time.Second
	nextRequestTS     int64 = 0
)

// Drainer converts tidb binlog to the specified DB sqls, and sync it to target DB
type Drainer struct {
	sync.Mutex

	cfg *Config

	schema *Schema
	meta   Meta

	translator *translator.Manager

	wg    sync.WaitGroup
	jobWg sync.WaitGroup

	input chan []byte

	store        kv.Storage
	toDB         *sql.DB
	binlogClient proto.BinlogClient

	done chan struct{}
	job  chan *job

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
func NewDrainer(cfg *Config, store kv.Storage, binlogClient proto.BinlogClient) *Drainer {
	drainer := new(Drainer)
	drainer.cfg = cfg
	drainer.store = store
	drainer.binlogClient = binlogClient
	drainer.meta = NewLocalMeta(cfg.Meta, cfg.InitTs)
	drainer.input = make(chan []byte, 1024)
	drainer.lastCount.Set(0)
	drainer.count.Set(0)
	drainer.insertCount.Set(0)
	drainer.updateCount.Set(0)
	drainer.deleteCount.Set(0)
	drainer.done = make(chan struct{})
	drainer.job = make(chan *job, 1024)
	drainer.ctx, drainer.cancel = context.WithCancel(context.Background())

	return drainer
}

// Start starts to sync.
func (d *Drainer) Start() error {
	err := d.meta.Load()
	if err != nil {
		return errors.Trace(err)
	}

	// sync the schema at meta.Pos
	d.schema, err = NewSchema(d.store, uint64(d.meta.Pos()))
	if err != nil {
		return errors.Trace(err)
	}

	d.wg.Add(1)

	err = d.run()
	if err != nil {
		return errors.Trace(err)
	}

	d.done <- struct{}{}

	return nil
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

func (d *Drainer) checkWait(job *job) bool {
	if job.tp == translator.DDL {
		return true
	}

	if d.meta.Check() {
		return true
	}

	return false
}

func (d *Drainer) addJob(job *job) error {
	d.jobWg.Add(1)
	d.job <- job

	wait := d.checkWait(job)
	if wait {
		d.jobWg.Wait()

		err := d.meta.Save(job.pos)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
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

func (d *Drainer) handleDDL(id int64, sql string) (string, string, bool, error) {
	job, err := d.getHistoryJob(id)
	if err != nil {
		return "", "", false, errors.Trace(err)
	}

	tmpDelay := retryTimeout
	for job == nil || job.State != model.JobDone {
		if job.State == model.JobCancelled {
			return "", "", false, nil
		}

		time.Sleep(tmpDelay)
		tmpDelay *= 2
		if tmpDelay > maxWaitGetJobTime {
			tmpDelay = maxWaitGetJobTime
		}

		job, err = d.getHistoryJob(id)
		if err != nil {
			return "", "", false, errors.Trace(err)
		}
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

type job struct {
	tp    translator.OpType
	sql   string
	args  []interface{}
	retry bool
	pos   int64
}

func newJob(tp translator.OpType, sql string, args []interface{}, retry bool, pos int64) *job {
	return &job{tp: tp, sql: sql, args: args, retry: retry, pos: pos}
}

func (d *Drainer) sync(db *sql.DB, jobChan chan *job) {
	defer d.wg.Done()

	idx := 0
	count := d.cfg.Batch
	sqls := make([]string, 0, count)
	args := make([][]interface{}, 0, count)
	lastSyncTime := time.Now()

	var err error
	for {
		select {
		case job, ok := <-jobChan:
			if !ok {
				return
			}

			idx++

			if job.tp == translator.DDL {
				err = executeSQLs(db, sqls, args, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}

				err = executeSQLs(db, []string{job.sql}, [][]interface{}{job.args}, false)
				if err != nil {
					if !ignoreDDLError(err) {
						log.Fatalf(errors.ErrorStack(err))
					} else {
						log.Warnf("[ignore ddl error][sql]%s[args]%v[error]%v", job.sql, job.args, err)
					}
				}

				idx = 0
				sqls = sqls[0:0]
				args = args[0:0]
				lastSyncTime = time.Now()
			} else {
				sqls = append(sqls, job.sql)
				args = append(args, job.args)
			}

			if idx >= count {
				err = executeSQLs(db, sqls, args, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}

				idx = 0
				sqls = sqls[0:0]
				args = args[0:0]
				lastSyncTime = time.Now()
			}

			d.addCount(job.tp)
			d.jobWg.Done()
		default:
			now := time.Now()
			if now.Sub(lastSyncTime) >= maxWaitTime {
				err = executeSQLs(db, sqls, args, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}

				idx = 0
				sqls = sqls[0:0]
				args = args[0:0]
				lastSyncTime = now
			}

			time.Sleep(waitTime)
		}
	}
}

func (d *Drainer) run() error {
	defer d.wg.Done()

	var err error
	var operations = []translator.OpType{translator.Insert, translator.Update, translator.Del}

	d.toDB, err = openDB(d.cfg.To.User, d.cfg.To.Password, d.cfg.To.Host, d.cfg.To.Port, d.cfg.DBType)
	if err != nil {
		return errors.Trace(err)
	}

	d.translator, err = translator.NewManager(d.cfg.DBType)
	if err != nil {
		return errors.Trace(err)
	}

	d.start = time.Now()
	d.lastTime = d.start

	d.wg.Add(1)
	go d.sync(d.toDB, d.job)

	d.wg.Add(1)
	go d.printStatus()

	d.wg.Add(1)
	nextRequestTS = d.meta.Pos()
	go d.onStream()

	for {
		var rawBinlog []byte

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

			mutations := preWrite.GetMutations()

			var (
				sqls   []string
				args   [][]interface{}
				bound  []int
				offset int
			)

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

					job := newJob(operations[offset], sqls[i], args[i], true, commitTS)
					err = d.addJob(job)
					if err != nil {
						return errors.Trace(err)
					}
				}

			}

		case pb.BinlogType_PreDDL:
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

				job := newJob(translator.DDL, sql, nil, false, commitTS)
				err = d.addJob(job)
				if err != nil {
					return errors.Trace(err)
				}

				log.Infof("[ddl][end]%s[pos]%v", sql, commitTS)
			}
		}
	}
}

func (d *Drainer) printStatus() {
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

func (d *Drainer) onStream() {
	defer d.wg.Done()

	var err error
	var count int
	var resp *proto.DumpBinlogResp

	tmpDelay := retryTimeout

	for {
		select {
		case <-d.ctx.Done():
			return
		default:
			if count > 0 {
				d.input <- resp.Payloads[len(resp.Payloads)-count]
				count--
				continue
			}

			req := &proto.DumpBinlogReq{BeginCommitTS: nextRequestTS, Limit: int32(d.cfg.RequestCount)}
			resp, err = d.binlogClient.DumpBinlog(context.Background(), req)
			if err != nil {
				log.Warning(err)
			}
			if err == nil && resp.Errmsg != "" {
				log.Warning(errors.New(resp.Errmsg))
			}

			if resp == nil {
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > maxWaitGetJobTime {
					tmpDelay = maxWaitGetJobTime
				}
			} else {
				tmpDelay = retryTimeout
				count = len(resp.Payloads)
				nextRequestTS = resp.EndCommitTS
			}
		}
	}
}

// Close closes syncer.
func (d *Drainer) Close() {
	d.Lock()
	defer d.Unlock()

	if d.store != nil {
		d.store.Close()
	}

	d.cancel()

	close(d.job)

	d.wg.Wait()

	closeDB(d.toDB)

	<-d.done
}
