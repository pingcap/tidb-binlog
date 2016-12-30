package cistern

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/codec"
	pb "github.com/pingcap/tipb/go-binlog"
)

const defaultBinlogChanSize int64 = 16 << 10

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
	conn      *grpc.ClientConn
	client    pb.PumpClient
	current   pb.Pos
	boltdb    store.Store
	tiStore   kv.Storage
	window    *DepositWindow
	timeout   time.Duration

	// pullBinlogs sends the binlogs to publish by it
	binlogChan chan *binlogEntity
	// the latestTS from tso
	latestTS int64
	// binlogs are complete before latestValidCommitTS
	latestValidCommitTS int64
	mu                  struct {
		sync.Mutex
		prewriteItems map[int64]*pb.Binlog
		binlogs       map[int64]*pb.Binlog
	}

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewPump returns an instance of Pump with opened gRPC connection
func NewPump(nodeID string, clusterID uint64, host string, timeout time.Duration, w *DepositWindow, pos pb.Pos, boltdb store.Store, tiStore kv.Storage) (*Pump, error) {
	conn, err := grpc.Dial(host, grpc.WithInsecure(), grpc.WithTimeout(timeout))
	if err != nil {
		return nil, errors.Annotatef(err, "failed to connect to pump node(%s) at host(%s)", nodeID, host)
	}
	return &Pump{
		nodeID:     nodeID,
		clusterID:  clusterID,
		conn:       conn,
		client:     pb.NewPumpClient(conn),
		current:    pos,
		boltdb:     boltdb,
		tiStore:    tiStore,
		window:     w,
		timeout:    timeout,
		binlogChan: make(chan *binlogEntity, defaultBinlogChanSize),
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
	p.mu.prewriteItems = make(map[int64]*pb.Binlog)
	p.mu.binlogs = make(map[int64]*pb.Binlog)
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
		p.mu.prewriteItems[b.StartTs] = b
	case pb.BinlogType_Commit, pb.BinlogType_Rollback:
		if co, ok := p.mu.prewriteItems[b.StartTs]; ok {
			if b.Tp == pb.BinlogType_Commit {
				co.CommitTs = b.CommitTs
				co.Tp = b.Tp
				p.mu.binlogs[co.CommitTs] = co
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
		binlogs     map[int64]*pb.Binlog
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
			// if the commitTs is larger than maxCommitTs, we would store all binlogs that already matched, lateValidCommitTs and savpoint
			if entity.commitTS > maxCommitTs {
				binlogs = p.getBinlogs(binlogs)
				maxCommitTs = entity.commitTS
				err := p.save(binlogs, maxCommitTs, entity.pos)
				if err != nil {
					log.Errorf("save binlogs and status error at postion (%v)", entity.pos)
				} else {
					binlogs = make(map[int64]*pb.Binlog)
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
func (p *Pump) query(t *tikv.LockResolver, binlog *pb.Binlog) bool {
	latestTs := atomic.LoadInt64(&p.latestTS)
	startTS := oracle.ExtractPhysical(uint64(binlog.StartTs)) / int64(time.Second/time.Millisecond)
	maxTS := oracle.ExtractPhysical(uint64(latestTs)) / int64(time.Second/time.Millisecond)
	if (maxTS - startTS) > maxTxnTimeout {
		if binlog.GetDdlJobId() == 0 {
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
				p.mu.binlogs[binlog.CommitTs] = binlog
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
func (p *Pump) getBinlogs(binlogs map[int64]*pb.Binlog) map[int64]*pb.Binlog {
	var tmpBinlogs map[int64]*pb.Binlog
	p.mu.Lock()
	tmpBinlogs = p.mu.binlogs
	p.mu.binlogs = make(map[int64]*pb.Binlog)
	p.mu.Unlock()
	if binlogs == nil {
		return tmpBinlogs
	}
	for ts, b := range tmpBinlogs {
		binlogs[ts] = b
	}
	return binlogs
}

func (p *Pump) save(items map[int64]*pb.Binlog, lastValidCommitTS int64, pos pb.Pos) error {
	err := p.saveItems(items)
	if err != nil {
		return errors.Trace(err)
	}
	err = p.updateSavepoint(pos)
	if err != nil {
		return errors.Trace(err)
	}

	latest := atomic.LoadInt64(&p.latestValidCommitTS)
	if latest < lastValidCommitTS {
		atomic.StoreInt64(&p.latestValidCommitTS, lastValidCommitTS)
	}

	return nil
}

func (p *Pump) saveItems(items map[int64]*pb.Binlog) error {
	jobs, err := p.grabDDLJobs(items)
	if err != nil {
		log.Errorf("grabDDLJobs error %v", errors.Trace(err))
	}

	err = p.storeDDLJobs(jobs)
	if err != nil {
		return errors.Trace(err)
	}

	err = p.store(items)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (p *Pump) store(items map[int64]*pb.Binlog) error {
	boundary := p.window.LoadLower()
	b := p.boltdb.NewBatch()

	for commitTS, item := range items {
		if commitTS < boundary {
			log.Errorf("FATAL ERROR: commitTs(%d) of binlog exceeds the lower boundary of window, may miss processing, ITEM(%v)",
				commitTS, item)
		}
		payload, err := item.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		key := codec.EncodeInt([]byte{}, commitTS)
		data, err := encodePayload(payload)
		if err != nil {
			return errors.Trace(err)
		}
		b.Put(key, data)
	}

	err := p.boltdb.Commit(binlogNamespace, b)
	return errors.Trace(err)
}

func (p *Pump) grabDDLJobs(items map[int64]*pb.Binlog) (map[int64]*model.Job, error) {
	res := make(map[int64]*model.Job)
	for ts, item := range items {
		if item.DdlJobId > 0 {
			job, err := p.getDDLJob(item.DdlJobId)
			if err != nil {
				return nil, errors.Trace(err)
			}
			for job == nil {
				select {
				case <-p.ctx.Done():
					return nil, errors.Trace(p.ctx.Err())
				case <-time.After(p.timeout):
					job, err = p.getDDLJob(item.DdlJobId)
					if err != nil {
						return nil, errors.Trace(err)
					}
				}
			}
			if job.State == model.JobCancelled {
				delete(items, ts)
			} else {
				res[item.DdlJobId] = job
			}
		}
	}
	return res, nil
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

func (p *Pump) storeDDLJobs(jobs map[int64]*model.Job) error {
	b := p.boltdb.NewBatch()
	for id, job := range jobs {
		if job.State == model.JobCancelled {
			continue
		}
		payload, err := job.Encode()
		if err != nil {
			return errors.Trace(err)
		}
		key := codec.EncodeInt([]byte{}, id)
		b.Put(key, payload)
	}
	err := p.boltdb.Commit(ddlJobNamespace, b)
	return errors.Trace(err)
}

func (p *Pump) updateSavepoint(pos pb.Pos) error {
	if ComparePos(pos, p.current) < 1 {
		return nil
	}

	data, err := pos.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	err = p.boltdb.Put(savepointNamespace, []byte(p.nodeID), data)
	if err != nil {
		return errors.Trace(err)
	}
	p.current = pos
	return nil
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
				log.Errorf("[Get pull binlogs stream]%v", err)
				time.Sleep(retryWaitTime)
				continue
			}

			pos, err = p.receiveBinlog(stream)
			if err != nil {
				if errors.Cause(err) != io.EOF {
					log.Errorf("[stream]%v", err)
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
			binlogEnt := &binlogEntity{
				tp:       b.Tp,
				startTS:  b.StartTs,
				commitTS: b.CommitTs,
				pos:      pos,
			}
			// send to publish goroutinue
			select {
			case <-p.ctx.Done():
				return pos, errors.Trace(err)
			case p.binlogChan <- binlogEnt:
			}
		}
	}
}

// GetLatestValidCommitTS returns the latest valid commit ts, the binlogs before this ts are complete
func (p *Pump) GetLatestValidCommitTS() int64 {
	return atomic.LoadInt64(&p.latestValidCommitTS)
}
