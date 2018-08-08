package drainer

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pump"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	pb "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// we wait waitMatchedTime for the match C binlog, atfer waitMatchedTime we try to query the status from tikv
var waitMatchedTime = 3 * time.Second

type binlogEntity struct {
	tp       pb.BinlogType
	startTS  int64
	commitTS int64
}

// Pump holds the connection to a pump node, and keeps the savepoint of binlog last read
type Pump struct {
	nodeID    string
	addr      string
	clusterID uint64
	// the current position that collector is working on
	currentPos int64
	// the latest binlog position that pump had handled
	latestPos int64
	tiStore   kv.Storage
	window    *DepositWindow
	timeout   time.Duration

	// the latestTS from tso
	latestTS int64
	// binlogs are complete before this latestValidCommitTS
	latestValidCommitTS int64

	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	isFinished int64
}

// NewPump returns an instance of Pump with opened gRPC connection
func NewPump(nodeID, addr string, clusterID uint64, timeout time.Duration, w *DepositWindow, tiStore kv.Storage, startTs int64) (*Pump, error) {
	nodeID, err := pump.FormatNodeID(nodeID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Pump{
		nodeID:     nodeID,
		addr:       addr,
		clusterID:  clusterID,
		currentPos: startTs,
		latestPos:  startTs,
		tiStore:    tiStore,
		window:     w,
		timeout:    timeout,
	}, nil
}

// Close closes all process goroutine, publish + pullBinlogs
func (p *Pump) Close() {
	log.Debugf("[pump %s] closing", p.nodeID)
	p.cancel()
	p.wg.Wait()
	log.Debugf("[pump %s] was closed", p.nodeID)
}

// PullBinlog return the chan to get item from pump
func (p *Pump) PullBinlog(pctx context.Context, last int64) chan MergeItem {
	p.ctx, p.cancel = context.WithCancel(pctx)
	ret := make(chan MergeItem)

	go func() {
		log.Debug("start PullBinlog pump: ", p.nodeID)
		defer func() {
			close(ret)
			log.Debug("start PullBinlog pump leave: ", p.nodeID)
		}()

		for {
			select {
			case <-p.ctx.Done():
				return
			default:
			}

			conn, err := grpc.Dial(p.addr, grpc.WithInsecure())
			if err != nil {
				log.Error(err)
				time.Sleep(time.Second)
				continue
			}

			defer conn.Close()
			cli := pb.NewPumpClient(conn)

			in := &pb.PullBinlogReq{
				ClusterID: p.clusterID,
				StartFrom: pb.Pos{Offset: last},
			}
			pullCli, err := cli.PullBinlogs(p.ctx, in)
			if err != nil {
				log.Error(err)
				time.Sleep(time.Second)
				continue
			}

			for {
				resp, err := pullCli.Recv()
				if err != nil {
					time.Sleep(time.Second)
					break
				}

				binlog := new(pb.Binlog)
				err = binlog.Unmarshal(resp.Entity.Payload)
				if err != nil {
					log.Error(err)
					continue
				}

				item := &binlogItem{
					binlog: binlog,
					nodeID: p.nodeID,
				}
				select {
				case ret <- item:
					last = binlog.CommitTs
				case <-p.ctx.Done():
				}
			}
		}
	}()

	return ret
}

// UpdateLatestTS updates the latest ts that query from pd
func (p *Pump) UpdateLatestTS(ts int64) {
	latestTS := atomic.LoadInt64(&p.latestTS)
	if ts > latestTS {
		atomic.StoreInt64(&p.latestTS, ts)
	}
}

func (p *Pump) grabDDLJobs(items map[int64]*binlogItem) error {
	var count int
	for ts, item := range items {
		b := item.binlog
		if b.DdlJobId > 0 {
			job, err := getDDLJob(p.tiStore, b.DdlJobId)
			if err != nil {
				return errors.Trace(err)
			}
			for job == nil {
				select {
				case <-p.ctx.Done():
					return errors.Trace(p.ctx.Err())
				case <-time.After(p.timeout):
					job, err = getDDLJob(p.tiStore, b.DdlJobId)
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

func (p *Pump) hadFinished(windowLower int64) bool {
	if p.latestValidCommitTS <= windowLower {
		return true
	}
	return false
}

// GetLatestValidCommitTS returns the latest valid commit ts, the binlogs before this ts are complete
func (p *Pump) GetLatestValidCommitTS() int64 {
	return atomic.LoadInt64(&p.latestValidCommitTS)
}
