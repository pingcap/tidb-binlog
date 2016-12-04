package cistern

import (
	"io"
	"math"
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

	buf                 *Buffer
	latestCommitTS      int64
	latestValidCommitTS int64
	mu                  struct {
		sync.Mutex
		cursor        int64
		prewriteItems map[int64]*pb.Binlog
		binlogs       map[int64]*pb.Binlog
	}

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewPump return an instance of Pump with opened gRPC connection
func NewPump(nodeID string, clusterID uint64, host string, timeout time.Duration, w *DepositWindow, pos pb.Pos, boltdb store.Store, tiStore kv.Storage) (*Pump, error) {
	conn, err := grpc.Dial(host, grpc.WithInsecure(), grpc.WithTimeout(timeout))
	if err != nil {
		return nil, errors.Annotatef(err, "failed to connect to pump node(%s) at host(%s)", nodeID, host)
	}
	return &Pump{
		nodeID:    nodeID,
		clusterID: clusterID,
		buf:       NewBuffer(),
		conn:      conn,
		client:    pb.NewPumpClient(conn),
		current:   pos,
		boltdb:    boltdb,
		tiStore:   tiStore,
		window:    w,
		timeout:   timeout,
	}, nil
}

// Close cuts off connection to pump server
func (p *Pump) Close() {
	p.cancel()
	p.wg.Wait()
}

// StartCollect start to handle the pump's binlogs
// 1. pull binlogs from pump, and put them in the Buffer
// 2. match p+c binlogs
// 3. forward the lower boundary and store the binlogs
func (p *Pump) StartCollect(pctx context.Context, t *tikv.LockResolver) {
	p.ctx, p.cancel = context.WithCancel(pctx)
	p.mu.prewriteItems = make(map[int64]*pb.Binlog)
	p.mu.binlogs = make(map[int64]*pb.Binlog)
	go p.match()
	go p.pullBinlogs()
	go p.publish(t)
}

// match is responsible for match p+c binlog and forward the pump's cursor.
// when it meet `end` of buffer , it would wait for a moment , then retry.
// At the same time, it updates the latestCommitTS of binlogs and hold the pre binlog that not matched
func (p *Pump) match() {
	end := p.buf.GetEndCursor()
	cursor := atomic.LoadInt64(&p.mu.cursor)
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		if cursor == end {
			end = p.buf.GetEndCursor()
			if cursor == end {
				time.Sleep(retryTimeout)
				continue
			}
		}

		item := p.buf.GetByIndex(cursor)
		b := new(pb.Binlog)
		err := b.Unmarshal(item.Payload)
		if err != nil {
			// skip?
			log.Errorf("unmarshal payload error, clusterID(%d), Pos(%v), error(%v)", p.clusterID, item.Pos, err)
			cursor++
			continue
		}
		p.mu.Lock()
		switch b.Tp {
		case pb.BinlogType_Prewrite:
			p.mu.prewriteItems[b.StartTs] = b
			p.updateLatest(b.StartTs)
		case pb.BinlogType_Commit, pb.BinlogType_Rollback:
			if co, ok := p.mu.prewriteItems[b.StartTs]; ok {
				if b.Tp == pb.BinlogType_Commit {
					co.CommitTs = b.CommitTs
					co.Tp = b.Tp
					p.mu.binlogs[co.CommitTs] = co
				}
				delete(p.mu.prewriteItems, b.StartTs)
			}
			p.updateLatest(b.CommitTs)
		default:
			log.Errorf("unrecognized binlog type(%d), clusterID(%d), Pos(%v) ", b.Tp, p.clusterID, item.Pos)
		}
		cursor++
		p.mu.cursor = cursor
		p.mu.Unlock()
	}
}

func (p *Pump) updateLatest(ts int64) {
	latestTS := atomic.LoadInt64(&p.latestCommitTS)
	if ts > latestTS {
		atomic.StoreInt64(&p.latestCommitTS, ts)
	}
}

func (p *Pump) publish(t *tikv.LockResolver) {
	start := p.buf.GetStartCursor()
	cursor := start
	var (
		minStartTs    int64
		maxCommitTs   int64
		isSavePoint   bool
		isStore       bool
		isQuery       bool
		pos           pb.Pos
		err           error
		binlogs       map[int64]*pb.Binlog
		prewriteItems map[int64]*pb.Binlog
	)
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		if start == cursor {
			if isQuery {
				binlogs = p.query(t, prewriteItems, binlogs)
				if err != nil {
					time.Sleep(retryTimeout)
					continue
				}
				isQuery = false
			}
			if isStore {
				err = p.save(binlogs, maxCommitTs, pos)
				if err != nil {
					log.Errorf("save binlogs(%v), latestValidCommitTS (%d) error: %v", binlogs, maxCommitTs, err)
					time.Sleep(retryTimeout)
					continue
				}
				isStore = false
			}

			prewriteItems, binlogs, cursor = p.getPumpStatus()
			if start == cursor {
				time.Sleep(retryTimeout)
				continue
			} else {
				isStore = true
				isQuery = true
				isSavePoint = false
				minStartTs = math.MaxInt64
				maxCommitTs = 0
			}
		}

		item := p.buf.Get()
		b := new(pb.Binlog)
		err = b.Unmarshal(item.Payload)
		if err != nil {
			p.buf.Next()
			start++
			log.Errorf("unmarshal payload error, clusterID(%d), Pos(%v), error: %v", p.clusterID, item.Pos, err)
			continue
		}
		switch b.Tp {
		case pb.BinlogType_Prewrite:
			_, ok := prewriteItems[b.StartTs]
			if ok {
				if b.StartTs < minStartTs {
					minStartTs = b.StartTs
				}
				if !isSavePoint {
					pos = item.Pos
					isSavePoint = true
				}
			}
		case pb.BinlogType_Commit, pb.BinlogType_Rollback:
			if b.CommitTs < minStartTs && b.CommitTs > maxCommitTs {
				maxCommitTs = b.CommitTs
			}
		}
		if !isSavePoint {
			pos = CalculateNextPos(item)
		}
		p.buf.Next()
		start++
	}
}

func (p *Pump) query(t *tikv.LockResolver, prewriteItems, binlogs map[int64]*pb.Binlog) map[int64]*pb.Binlog {
	for _, b := range prewriteItems {
		latestTs := atomic.LoadInt64(&p.latestCommitTS)
		startTS := oracle.ExtractPhysical(uint64(b.StartTs)) / int64(time.Second/time.Millisecond)
		maxTS := oracle.ExtractPhysical(uint64(latestTs)) / int64(time.Second/time.Millisecond)
		if (maxTS - startTS) > 600 {
			if b.GetDdlJobId() == 0 {
				primaryKey := b.GetPrewriteKey()
				status, err := t.GetTxnStatus(uint64(b.StartTs), primaryKey)
				if err != nil {
					log.Errorf("get item's(%v) txn status error: %v", b, err)
					continue
				}
				p.mu.Lock()
				if status.IsCommitted() {
					b.CommitTs = int64(status.CommitTS())
					b.Tp = pb.BinlogType_Commit
					binlogs[b.CommitTs] = b
					delete(p.mu.binlogs, b.CommitTs)
				}
				delete(p.mu.prewriteItems, b.StartTs)
				p.mu.Unlock()
			} else {
				// todo: get ddl from history job or continue waiting?
				log.Errorf("some prewrite DDL items remain single after waiting for a long time, item(%v)", b)
			}
		}
	}
	return binlogs
}

func (p *Pump) getPumpStatus() (map[int64]*pb.Binlog, map[int64]*pb.Binlog, int64) {
	prewriteItems := make(map[int64]*pb.Binlog)
	p.mu.Lock()
	binlogs := p.mu.binlogs
	cursor := p.mu.cursor
	p.mu.binlogs = make(map[int64]*pb.Binlog)
	for ts, item := range p.mu.prewriteItems {
		prewriteItems[ts] = item
	}
	p.mu.Unlock()
	return prewriteItems, binlogs, cursor
}

func (p *Pump) save(items map[int64]*pb.Binlog, lastValidCommitTS int64, pos pb.Pos) error {
	err := p.saveItems(items)
	if err != nil {
		return errors.Trace(err)
	}
	latest := atomic.LoadInt64(&p.latestValidCommitTS)
	if latest < lastValidCommitTS {
		atomic.StoreInt64(&p.latestValidCommitTS, lastValidCommitTS)
	}

	err = p.updateSavepoint(pos)
	if err != nil {
		return errors.Trace(err)
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
		if err := decodeJob(job); err != nil {
			return errors.Trace(err)
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
				time.Sleep(retryTimeout)
				continue
			}

			pos, err = p.receiveBinlog(stream)
			if err != nil {
				if errors.Cause(err) != io.EOF {
					log.Errorf("[stream]%v", err)
				}
				time.Sleep(retryTimeout)
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
			break
		}

		if resp.Errmsg != "" {
			err = errors.Errorf("resp has error message: %s", resp.Errmsg)
			break
		}

		err = p.buf.Store(p.ctx, resp.Entity)
		if err != nil {
			break
		}
		pos = CalculateNextPos(resp.Entity)
	}
	return pos, errors.Trace(err)
}

// GetLatestCommitTS returns the latest commit ts
func (p *Pump) GetLatestCommitTS() int64 {
	return atomic.LoadInt64(&p.latestCommitTS)
}

// GetLatestValidCommitTS returns the latest valid commit ts, the binlogs before this ts are complete
func (p *Pump) GetLatestValidCommitTS() int64 {
	return atomic.LoadInt64(&p.latestValidCommitTS)
}
