package drainer

import (
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/pump"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	pb "github.com/pingcap/tipb/go-binlog"
)

type binlogEntity struct {
	tp       pb.BinlogType
	startTS  int64
	commitTS int64
	pos      pb.Pos
}

// Pump holds the connection to a pump node, and keeps the savepoint of binlog last read
type Pump struct {
	nodeID    string
	clusterID uint64
	consumer  sarama.Consumer
	// the current position that collector is working on
	currentPos pb.Pos
	// the latest binlog position that pump had handled
	latestPos pb.Pos
	// store binlogs in a heap
	bh      *binlogHeap
	tiStore kv.Storage
	window  *DepositWindow
	timeout time.Duration

	// pullBinlogs sends the binlogs to publish function by this channel
	binlogChan chan *binlogEntity
	//binlogChan chan *binlogItem

	// the latestTS from tso
	latestTS int64
	// binlogs are complete before this latestValidCommitTS
	latestValidCommitTS int64
	mu                  struct {
		sync.Mutex
		prewriteItems map[int64]*binlogItem
		binlogs       map[int64]*binlogItem
	}

	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	isFinished int64

	filter       *filter
	initCommitTS int64
	cp           checkpoint.CheckPoint
}

// NewPump returns an instance of Pump with opened gRPC connection
func NewPump(nodeID string, clusterID uint64, kafkaAddrs []string, timeout time.Duration, w *DepositWindow, tiStore kv.Storage, pos pb.Pos, filter *filter, cp checkpoint.CheckPoint) (*Pump, error) {
	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer(kafkaAddrs, kafkaCfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	initCommitTS, _ := cp.Pos()

	return &Pump{
		nodeID:       nodeID,
		clusterID:    clusterID,
		consumer:     consumer,
		currentPos:   pos,
		latestPos:    pos,
		bh:           newBinlogHeap(maxBinlogItemCount),
		tiStore:      tiStore,
		window:       w,
		timeout:      timeout,
		binlogChan:   make(chan *binlogEntity, maxBinlogItemCount),
		cp:           cp,
		initCommitTS: initCommitTS,
		filter:       filter,
	}, nil
}

// Close closes all process goroutine, publish + pullBinlogs
func (p *Pump) Close() {
	p.cancel()
	p.consumer.Close()
	p.wg.Wait()
}

// StartCollect starts to process the pump's binlogs
// 1. pullBinlogs pulls binlogs from pump, match p+c binlog by using prewriteItems map
// 2. publish query the non-match pre binlog and forwards the lower boundary, push them into a heap
func (p *Pump) StartCollect(pctx context.Context, t *tikv.LockResolver) {
	p.ctx, p.cancel = context.WithCancel(pctx)

	p.mu.prewriteItems = make(map[int64]*binlogItem)
	p.mu.binlogs = make(map[int64]*binlogItem)
	go p.pullBinlogs()
	go p.publish(t)
}

func (p *Pump) needFilter(item *binlogItem) bool {
	binlog := item.binlog
	jobID := binlog.ddlJobID
	preWrite := binlog.prewriteValue

	if p.initCommitTS > 0 && binlog.commitTs < p.initCommitTS {
		return true
	}

	newMumation := make([]pb.TableMutation, 0, len(preWrite.Mutations))

	if jobID == 0 {
		for _, mutation := range preWrite.Mutations {
			tableID := mutation.TableId

			schemaName, tableName, ok := p.filter.schema.SchemaAndTableName(tableID)
			if !ok {
				log.Debugf("can't find tableID %d int schema", tableID)
				newMumation = append(newMumation, mutation)
				continue
			}

			if p.filter.SkipSchemaAndTable(schemaName, tableName) {
				log.Debugf("skip %s.%s dml", schemaName, tableName)
				continue
			}

			newMumation = append(newMumation, mutation)
		}
		if len(newMumation) == 0 {
			return true
		}
		binlog.setMumations(newMumation)
		return false
	}
	/*
	sql, err := p.handleDDL(item.job)
	if err != nil {
		log.Errorf("handleDDL error: %v", errors.Trace(err))
	}
	if sql == "" {
		return true
	}
	*/

	return false
}

// match is responsible for match p+c binlog
func (p *Pump) match(ent pb.Entity) *pb.Binlog {
	b := new(pb.Binlog)
	err := b.Unmarshal(ent.Payload)
	if err != nil {
		// skip?
		log.Errorf("unmarshal payload error, clusterID(%d), Pos(%v), error(%v)", p.clusterID, ent.Pos, err)
		return nil
	}

	p.mu.Lock()
	switch b.Tp {
	case pb.BinlogType_Prewrite:
		pos := pb.Pos{Suffix: ent.Pos.Suffix, Offset: ent.Pos.Offset}
		p.mu.prewriteItems[b.StartTs] = newBinlogItem(b, pos, p.nodeID)
	case pb.BinlogType_Commit, pb.BinlogType_Rollback:
		if co, ok := p.mu.prewriteItems[b.StartTs]; ok {
			if b.Tp == pb.BinlogType_Commit {
				co.binlog.commitTs = b.CommitTs
				co.binlog.tp = b.Tp
				p.mu.binlogs[co.binlog.commitTs] = co
			}
			delete(p.mu.prewriteItems, b.StartTs)
		}
	default:
		log.Errorf("unrecognized binlog type(%d), clusterID(%d), Pos(%v) ", b.Tp, p.clusterID, ent.Pos)
	}
	p.mu.Unlock()
	return b
}

// UpdateLatestTS updates the latest ts that query from pd
func (p *Pump) UpdateLatestTS(ts int64) {
	latestTS := atomic.LoadInt64(&p.latestTS)
	if ts > latestTS {
		atomic.StoreInt64(&p.latestTS, ts)
	}
}

// publish finds the maxValidCommitts and blocks when it meets a preBinlog
func (p *Pump) publish(t *tikv.LockResolver) {
	p.wg.Add(1)
	defer p.wg.Done()
	var (
		maxCommitTs int64
		entity      *binlogEntity
		binlogs     map[int64]*binlogItem
	)
	for {
		select {
		case <-p.ctx.Done():
			return
		case entity = <-p.binlogChan:
		}

		//switch entity.tp {
		switch entity.tp {
		case pb.BinlogType_Prewrite:
			// while we meet the prebinlog we must find it's mathced commit binlog
			p.mustFindCommitBinlog(t, entity.startTS)
		case pb.BinlogType_Commit, pb.BinlogType_Rollback:
			// if the commitTs is larger than maxCommitTs,
			// we would publish all binlogs:
			// 1. push binlog that matched into a heap
			// 2. update lateValidCommitTs
			if entity.commitTS > maxCommitTs {
				binlogs = p.getBinlogs(binlogs)
				maxCommitTs = entity.commitTS
				err := p.publishBinlogs(binlogs, maxCommitTs)
				if err != nil {
					log.Errorf("save binlogs and status error at ts(%v)", entity.commitTS)
				} else {
					binlogs = make(map[int64]*binlogItem)
				}
			}
		}

		// update latestPos
		if ComparePos(entity.pos, p.latestPos) > 0 {
			p.latestPos = entity.pos
		}
	}
}

func (p *Pump) mustFindCommitBinlog(t *tikv.LockResolver, startTS int64) {
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		b, ok := p.getPrewriteBinlogEntity(startTS)
		if ok {
			time.Sleep(waitTime)
			// check again after sleep a moment
			b, ok = p.getPrewriteBinlogEntity(startTS)
			if ok {
				if ok := p.query(t, b); !ok {
					continue
				}
			}
		}
		return
	}
}

// query binlog's commit status from tikv client, return true if it already commit or rollback
func (p *Pump) query(t *tikv.LockResolver, b *binlogItem) bool {
	binlog := b.binlog
	latestTs := atomic.LoadInt64(&p.latestTS)
	startTS := oracle.ExtractPhysical(uint64(binlog.startTs)) / int64(time.Second/time.Millisecond)
	maxTS := oracle.ExtractPhysical(uint64(latestTs)) / int64(time.Second/time.Millisecond)
	if (maxTS - startTS) > maxTxnTimeout {
		if binlog.ddlJobID == 0 {
			tikvQueryCount.Add(1)
			primaryKey := binlog.prewriteKey
			status, err := t.GetTxnStatus(uint64(binlog.startTs), primaryKey)
			if err != nil {
				log.Errorf("get item's(%v) txn status error: %v", binlog, err)
				return false
			}
			ts := binlog.startTs
			p.mu.Lock()
			if status.IsCommitted() {
				binlog.commitTs = int64(status.CommitTS())
				binlog.tp = pb.BinlogType_Commit
				p.mu.binlogs[binlog.commitTs] = b
			}
			delete(p.mu.prewriteItems, ts)
			p.mu.Unlock()
			return true
		}
		// todo: get ddl from history job or continue waiting?
		log.Errorf("some prewrite DDL items remain single after waiting for a long time, item(%v)", binlog)
		return false
	}
	return false
}

// get all binlogs that don't store in boltdb
func (p *Pump) getBinlogs(binlogs map[int64]*binlogItem) map[int64]*binlogItem {
	var tmpBinlogs map[int64]*binlogItem
	p.mu.Lock()
	tmpBinlogs = p.mu.binlogs
	p.mu.binlogs = make(map[int64]*binlogItem)
	p.mu.Unlock()
	if binlogs == nil {
		return tmpBinlogs
	}
	for ts, b := range tmpBinlogs {
		binlogs[ts] = b
	}
	return binlogs
}

func (p *Pump) publishBinlogs(items map[int64]*binlogItem, lastValidCommitTS int64) error {
	err := p.publishItems(items)
	if err != nil {
		return errors.Trace(err)
	}

	// this judgment seems to be unnecessary, but to ensure safety
	latest := atomic.LoadInt64(&p.latestValidCommitTS)
	if latest < lastValidCommitTS {
		atomic.StoreInt64(&p.latestValidCommitTS, lastValidCommitTS)
	}

	return nil
}

func (p *Pump) publishItems(items map[int64]*binlogItem) error {
	err := p.grabDDLJobs(items)
	if err != nil {
		log.Errorf("grabDDLJobs error %v", errors.Trace(err))
		return errors.Trace(err)
	}

	p.putIntoHeap(items)
	binlogCounter.Add(float64(len(items)))
	return nil
}

func (p *Pump) putIntoHeap(items map[int64]*binlogItem) {
	boundary := p.window.LoadLower()
	var errorBinlogs int

	for commitTS, item := range items {
		if commitTS < boundary {
			errorBinlogs++
			log.Errorf("FATAL ERROR: commitTs(%d) of binlog exceeds the lower boundary of window %d, may miss processing, ITEM(%v)", commitTS, boundary, item)
		}

		if p.needFilter(item) {
			continue
		}
		p.bh.push(p.ctx, item)
	}

	errorBinlogCount.Add(float64(errorBinlogs))
}

func (p *Pump) grabDDLJobs(items map[int64]*binlogItem) error {
	var count int
	for ts, item := range items {
		b := item.binlog
		if b.ddlJobID > 0 {
			job, err := p.getDDLJob(b.ddlJobID)
			if err != nil {
				return errors.Trace(err)
			}
			for job == nil {
				select {
				case <-p.ctx.Done():
					return errors.Trace(p.ctx.Err())
				case <-time.After(p.timeout):
					job, err = p.getDDLJob(b.ddlJobID)
					if err != nil {
						return errors.Trace(err)
					}
				}
			}
			if job.State == model.JobStateCancelled {
				delete(items, ts)
			} else {
				item.SetJob(job)
				count++
			}
		}
	}
	ddlJobsCounter.Add(float64(count))
	return nil
}

func (p *Pump) getDDLJob(id int64) (*model.Job, error) {
	version, err := p.tiStore.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapshot, err := p.tiStore.GetSnapshot(version)
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapMeta := meta.NewSnapshotMeta(snapshot)
	job, err := snapMeta.GetHistoryDDLJob(id)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return job, nil
}

func (p *Pump) collectBinlogs(windowLower, windowUpper int64) binlogItems {
	var bs binlogItems
	item := p.bh.pop()
	for item != nil && item.binlog.commitTs <= windowUpper {
		// make sure to discard old binlogs whose commitTS is earlier or equal minTS
		if item.binlog.commitTs > windowLower {
			bs = append(bs, item)
		}
		// update pump's current position
		if ComparePos(p.currentPos, item.pos) == -1 {
			p.currentPos = item.pos
		}
		item = p.bh.pop()
	}

	if item != nil {
		p.bh.push(p.ctx, item)
	}
	return bs
}

func (p *Pump) hadFinished(pos pb.Pos, windowLower int64) bool {
	if ComparePos(p.latestPos, pos) >= 0 && p.latestValidCommitTS <= windowLower {
		return true
	}
	return false
}

// pull binlogs in the streaming way, and match them
func (p *Pump) pullBinlogs() {
	p.wg.Add(1)
	defer p.wg.Done()
	var err error
	var stream sarama.PartitionConsumer
	topic := pump.TopicName(strconv.FormatUint(p.clusterID, 10), p.nodeID)
	pos := p.currentPos

	for {
		select {
		case <-p.ctx.Done():
			return
		default:
			stream, err = p.consumer.ConsumePartition(topic, pump.DefaultTopicPartition(), pos.Offset)
			if err != nil {
				log.Warningf("[get consumer partition client error %s] %v", p.nodeID, err)
				time.Sleep(waitTime)
				continue
			}

			pos, err = p.receiveBinlog(stream, pos)
			if err != nil {
				if errors.Cause(err) != io.EOF {
					log.Warningf("[stream] node %s, pos %+v, error %v", p.nodeID, pos, err)
				}
				time.Sleep(waitTime)
				continue
			}
		}
	}
}

func (p *Pump) receiveBinlog(stream sarama.PartitionConsumer, pos pb.Pos) (pb.Pos, error) {
	defer stream.Close()

	for {
		var payload []byte
		select {
		case <-p.ctx.Done():
			return pos, p.ctx.Err()
		case consumerErr := <-stream.Errors():
			return pos, errors.Errorf("consumer %v", consumerErr)
		case msg := <-stream.Messages():
			pos.Offset = msg.Offset
			payload = msg.Value
			messageCounter.Add(1)
		}

		entity := pb.Entity{
			Pos:     pos,
			Payload: payload,
		}

		b := p.match(entity)
		if b != nil {
			binlogEnt := &binlogEntity{
				tp:       b.Tp,
				startTS:  b.StartTs,
				commitTS: b.CommitTs,
				pos:      pos,
			}
			// send to publish goroutinue
			select {
			case <-p.ctx.Done():
				return pos, errors.Trace(p.ctx.Err())
			case p.binlogChan <- binlogEnt:
			}
		}
	}
}

func (p *Pump) getPrewriteBinlogEntity(startTS int64) (b *binlogItem, ok bool) {
	p.mu.Lock()
	b, ok = p.mu.prewriteItems[startTS]
	p.mu.Unlock()
	return
}

// GetLatestValidCommitTS returns the latest valid commit ts, the binlogs before this ts are complete
func (p *Pump) GetLatestValidCommitTS() int64 {
	return atomic.LoadInt64(&p.latestValidCommitTS)
}

func (p *Pump) handleDDL(job *model.Job) (string, error) {
	log.Infof("handle ddl query %s", job.Query)
	if job.State == model.JobStateCancelled {
		return "", nil
	}

	sql := job.Query
	if sql == "" {
		return "", errors.Errorf("[ddl job sql miss]%+v", job)
	}

	switch job.Type {
	case model.ActionCreateSchema:
		// get the DBInfo from job rawArgs
		schema := job.BinlogInfo.DBInfo
		if filterIgnoreSchema(schema, p.filter.ignoreDBs) {
			p.filter.schema.AddIgnoreSchema(schema)
			return "", nil
		}

		err := p.filter.schema.CreateSchema(schema)
		if err != nil {
			return "", errors.Trace(err)
		}

		return sql, nil

	case model.ActionDropSchema:
		_, ok := p.filter.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			p.filter.schema.DropIgnoreSchema(job.SchemaID)
			return "", nil
		}

		_, err := p.filter.schema.DropSchema(job.SchemaID)
		if err != nil {
			return "", errors.Trace(err)
		}

		return sql, nil

	case model.ActionRenameTable:
		// ignore schema doesn't support reanme ddl
		_, ok := p.filter.schema.SchemaByTableID(job.TableID)
		if !ok {
			return "", errors.NotFoundf("table(%d) or it's schema", job.TableID)
		}
		_, ok = p.filter.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", errors.Errorf("ignore schema %d doesn't support rename ddl sql %s", job.SchemaID, sql)
		}
		// first drop the table
		_, err := p.filter.schema.DropTable(job.TableID)
		if err != nil {
			return "", errors.Trace(err)
		}
		// create table
		table := job.BinlogInfo.TableInfo
		schema, ok := p.filter.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err = p.filter.schema.CreateTable(schema, table)
		if err != nil {
			return "", errors.Trace(err)
		}

		return sql, nil

	case model.ActionCreateTable:
		table := job.BinlogInfo.TableInfo
		if table == nil {
			return "", errors.NotFoundf("table %d", job.TableID)
		}

		_, ok := p.filter.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", nil
		}

		schema, ok := p.filter.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err := p.filter.schema.CreateTable(schema, table)
		if err != nil {
			return "", errors.Trace(err)
		}

		return sql, nil

	case model.ActionDropTable:
		_, ok := p.filter.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", nil
		}

		_, ok = p.filter.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		_, err := p.filter.schema.DropTable(job.TableID)
		if err != nil {
			return "", errors.Trace(err)
		}

		return sql, nil

	case model.ActionTruncateTable:
		_, ok := p.filter.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", nil
		}

		schema, ok := p.filter.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		_, err := p.filter.schema.DropTable(job.TableID)
		if err != nil {
			return "", errors.Trace(err)
		}

		table := job.BinlogInfo.TableInfo
		if table == nil {
			return "", errors.NotFoundf("table %d", job.TableID)
		}

		err = p.filter.schema.CreateTable(schema, table)
		if err != nil {
			return "", errors.Trace(err)
		}

		return sql, nil

	default:
		tbInfo := job.BinlogInfo.TableInfo
		if tbInfo == nil {
			return "", errors.NotFoundf("table %d", job.TableID)
		}

		_, ok := p.filter.schema.IgnoreSchemaByID(job.SchemaID)
		if ok {
			return "", nil
		}

		_, ok = p.filter.schema.SchemaByID(job.SchemaID)
		if !ok {
			return "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err := p.filter.schema.ReplaceTable(tbInfo)
		if err != nil {
			return "", errors.Trace(err)
		}

		return sql, nil
	}
}
