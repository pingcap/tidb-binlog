package drainer

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	pb "github.com/pingcap/tipb/go-binlog"
)

const defaultBinlogChanLength int64 = 16 << 12

// Pump holds the connection to a pump node, and keeps the savepoint of binlog last read
type Pump struct {
	nodeID    string
	clusterID uint64
	conn      *grpc.ClientConn
	client    pb.PumpClient
	current   pb.Pos
	// use heap to store binlogs in memory
	bh      *binlogHeap
	tiStore kv.Storage
	window  *DepositWindow
	timeout time.Duration

	// pullBinlogs sends the binlogs to publish by it
	binlogChan chan *pb.Binlog
	// the latestTS from tso
	latestTS int64
	// binlogs are complete before latestValidCommitTS
	latestValidCommitTS int64
	mu                  struct {
		sync.Mutex
		prewriteItems map[int64]*binlogItem
		binlogs       map[int64]*binlogItem
	}

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewPump returns an instance of Pump with opened gRPC connection
func NewPump(nodeID string, clusterID uint64, host string, timeout time.Duration, w *DepositWindow, tiStore kv.Storage, pos pb.Pos) (*Pump, error) {
	conn, err := grpc.Dial(host, grpc.WithInsecure(), grpc.WithTimeout(timeout))
	if err != nil {
		return nil, errors.Annotatef(err, "failed to connect to pump node(%s) at host(%s)", nodeID, host)
	}
	return &Pump{
		nodeID:     nodeID,
		clusterID:  clusterID,
		conn:       conn,
		current:    pos,
		client:     pb.NewPumpClient(conn),
		bh:         newBinlogHeap(),
		tiStore:    tiStore,
		window:     w,
		timeout:    timeout,
		binlogChan: make(chan *pb.Binlog, defaultBinlogChanLength),
	}, nil
}

// Close closes all process goroutine, publish + pullBinlogs
func (p *Pump) Close() {
	p.cancel()
	p.wg.Wait()
}

// StartCollect starts to process the pump's binlogs
// 1. pullBinlogs pulls binlogs from pump, match p+c binlog by using prewriteItems map
// 2. publish query the non-match pre binlog and forwards the lower boundary, store the binlogs
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
		entity      *pb.Binlog
		binlogs     map[int64]*binlogItem
	)
	for {
		select {
		case <-p.ctx.Done():
			return
		case entity = <-p.binlogChan:
		}

		switch entity.Tp {
		case pb.BinlogType_Prewrite:
			// while we meet the prebinlog we must find it's mathced commit binlog
			p.mustFindCommitBinlog(t, entity.StartTs)
		case pb.BinlogType_Commit, pb.BinlogType_Rollback:
			// if the commitTs is larger than maxCommitTs, we would store all binlogs that already matched, lateValidCommitTs and savpoint
			if entity.CommitTs > maxCommitTs {
				binlogs = p.getBinlogs(binlogs)
				maxCommitTs = entity.CommitTs
				err := p.publishBinlogs(binlogs, maxCommitTs)
				if err != nil {
					log.Errorf("save binlogs and status error at ts(%v)", entity.CommitTs)
				} else {
					binlogs = make(map[int64]*binlogItem)
				}
			}
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

		p.mu.Lock()
		b, ok := p.mu.prewriteItems[startTS]
		p.mu.Unlock()
		if ok {
			if ok := p.query(t, b); !ok {
				time.Sleep(retryWaitTime)
				continue
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

	err = p.putIntoHeap(items)
	if err != nil {
		return errors.Trace(err)
	}

	binlogCounter.Add(float64(len(items)))
	return nil
}

func (p *Pump) putIntoHeap(items map[int64]*binlogItem) error {
	boundary := p.window.LoadLower()
	var errorBinlogs int

	for commitTS, item := range items {
		if commitTS < boundary {
			errorBinlogs++
			log.Errorf("FATAL ERROR: commitTs(%d) of binlog exceeds the lower boundary of window, may miss processing, ITEM(%v)",
				commitTS, item)
		}
		p.bh.push(item)
	}

	errorBinlogCount.Add(float64(errorBinlogs))
	return nil
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
			if job.State == model.JobCancelled {
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

func (p *Pump) collectBinlogs(maxTS int64) binlogItems {
	var bs binlogItems
	item := p.bh.pop()
	for item != nil && item.binlog.CommitTs <= maxTS {
		bs = append(bs, item)
		// update pump's current position
		if ComparePos(p.current, item.pos) == -1 {
			p.current = item.pos
		}
		item = p.bh.pop()
	}

	if item != nil {
		p.bh.push(item)
	}
	return bs
}

// pull binlogs in the streaming way, and match them
func (p *Pump) pullBinlogs() {
	p.wg.Add(1)
	defer p.wg.Done()
	var err error
	var stream pb.Pump_PullBinlogsClient
	pos := p.current

	for {
		select {
		case <-p.ctx.Done():
			return
		default:
			req := &pb.PullBinlogReq{StartFrom: pos, ClusterID: p.clusterID}
			stream, err = p.client.PullBinlogs(p.ctx, req)
			if err != nil {
				log.Warningf("[Get pull binlogs stream]%v", err)
				time.Sleep(retryWaitTime)
				continue
			}

			pos, err = p.receiveBinlog(stream)
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

func (p *Pump) receiveBinlog(stream pb.Pump_PullBinlogsClient) (pb.Pos, error) {
	var err error
	var pos pb.Pos
	var resp *pb.PullBinlogResp

	for {
		resp, err = stream.Recv()
		if err != nil {
			return pos, errors.Trace(err)
		}

		pos = CalculateNextPos(resp.Entity)
		b := p.match(resp.Entity)
		if b != nil {
			// send to publish goroutinue
			select {
			case <-p.ctx.Done():
				return pos, errors.Trace(err)
			case p.binlogChan <- b:
			}
		}
	}
}

// GetLatestValidCommitTS returns the latest valid commit ts, the binlogs before this ts are complete
func (p *Pump) GetLatestValidCommitTS() int64 {
	return atomic.LoadInt64(&p.latestValidCommitTS)
}
