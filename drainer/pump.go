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
}

// NewPump returns an instance of Pump with opened gRPC connection
func NewPump(nodeID string, clusterID uint64, kafkaAddrs []string, timeout time.Duration, w *DepositWindow, tiStore kv.Storage, pos pb.Pos) (*Pump, error) {
	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer(kafkaAddrs, kafkaCfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Pump{
		nodeID:     nodeID,
		clusterID:  clusterID,
		consumer:   consumer,
		currentPos: pos,
		latestPos:  pos,
		bh:         newBinlogHeap(maxBinlogItemCount),
		tiStore:    tiStore,
		window:     w,
		timeout:    timeout,
		binlogChan: make(chan *binlogEntity, maxBinlogItemCount),
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

// match is responsible for match p+c binlog.
func (p *Pump) match(ent pb.Entity) *pb.Binlog {
	b := new(pb.Binlog)
	err := b.Unmarshal(ent.Payload)
	if err != nil {
		errorBinlogCount.Add(1)
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
				co.binlog.CommitTs = b.CommitTs
				co.binlog.Tp = b.Tp
				p.mu.binlogs[co.binlog.CommitTs] = co
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
	startTS := oracle.ExtractPhysical(uint64(binlog.StartTs)) / int64(time.Second/time.Millisecond)
	maxTS := oracle.ExtractPhysical(uint64(latestTs)) / int64(time.Second/time.Millisecond)
	if (maxTS - startTS) > maxTxnTimeout {
		if binlog.GetDdlJobId() == 0 {
			//log.Infof("binlog (%d) need to query tikv", binlog.StartTs)
			tikvQueryCount.Add(1)
			primaryKey := binlog.GetPrewriteKey()
			status, err := t.GetTxnStatus(uint64(binlog.StartTs), primaryKey)
			if err != nil {
				log.Errorf("get item's(%v) txn status error: %v", binlog, err)
				return false
			}
			ts := binlog.StartTs
			p.mu.Lock()
			if status.IsCommitted() {
				binlog.CommitTs = int64(status.CommitTS())
				binlog.Tp = pb.BinlogType_Commit
				p.mu.binlogs[binlog.CommitTs] = b
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
	publishBinlogCounter.WithLabelValues(p.nodeID).Add(float64(len(items)))
	return nil
}

func (p *Pump) putIntoHeap(items map[int64]*binlogItem) {
	boundary := p.window.LoadLower()
	var errorBinlogs int

	for commitTS, item := range items {
		if commitTS < boundary {
			errorBinlogs++
			log.Errorf("FATAL ERROR: commitTs(%d) of binlog exceeds the lower boundary of window %d, may miss processing, ITEM(%v)", commitTS, boundary, item)
			// if we meet a smaller binlog, we should ignore it. because we have published binlogs that before window low boundary
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
		if b.DdlJobId > 0 {
			job, err := p.getDDLJob(b.DdlJobId)
			if err != nil {
				return errors.Trace(err)
			}
			for job == nil {
				select {
				case <-p.ctx.Done():
					return errors.Trace(p.ctx.Err())
				case <-time.After(p.timeout):
					job, err = p.getDDLJob(b.DdlJobId)
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
	item := p.bh.peek()
	for item != nil && item.binlog.CommitTs <= windowUpper {
		// make sure to discard old binlogs whose commitTS is earlier or equal minTS
		if item.binlog.CommitTs > windowLower {
			bs = append(bs, item)
		}
		// update pump's current position
		if ComparePos(p.currentPos, item.pos) == -1 {
			p.currentPos = item.pos
		}
		_ = p.bh.pop()
		item := p.bh.peek()
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
			log.Infof("consume from topic %s partition %d offset %d", topic, pump.DefaultTopicPartition(), pos.Offset)
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
		var (
			payload   []byte
			beginTime = time.Now()
		)
		select {
		case <-p.ctx.Done():
			return pos, p.ctx.Err()
		case consumerErr := <-stream.Errors():
			return pos, errors.Errorf("consumer %v", consumerErr)
		case msg := <-stream.Messages():
			pos.Offset = msg.Offset
			payload = msg.Value
		}
		readBinlogHistogram.WithLabelValues(p.nodeID).Observe(time.Since(beginTime).Seconds())
		readBinlogSizeHistogram.WithLabelValues(p.nodeID).Observe(float64(len(payload)))

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
