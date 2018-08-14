package drainer

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pump"
	"github.com/pingcap/tidb/kv"
	pb "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

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
	// the latest binlog ts that pump had handled
	latestTS int64
	tiStore  kv.Storage
	timeout  time.Duration

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	isPaused int32
}

// NewPump returns an instance of Pump with opened gRPC connection
func NewPump(nodeID, addr string, clusterID uint64, timeout time.Duration, startTs int64) *Pump {
	nodeID, err := pump.FormatNodeID(nodeID)
	if err != nil {
		log.Warnf("pump 's node id %s maybe illegal", nodeID)
	}

	return &Pump{
		nodeID:    nodeID,
		addr:      addr,
		clusterID: clusterID,
		latestTS:  startTs,
		timeout:   timeout,
	}
}

// Close closes all process goroutine.
func (p *Pump) Close() {
	log.Debugf("[pump %s] closing", p.nodeID)
	p.cancel()
	p.wg.Wait()
	log.Debugf("[pump %s] was closed", p.nodeID)
}

// Pause sets isPaused to 1, and stop pull binlog from pump.
func (p *Pump) Pause() {
	if atomic.CompareAndSwapInt32(&p.isPaused, 0, 1) {
		log.Infof("pause pull binlog from pump %s", p.nodeID)
	}
}

// Continue sets isPaused to 0, and continue pull binlog from pump.
func (p *Pump) Continue() {
	if atomic.CompareAndSwapInt32(&p.isPaused, 1, 0) {
		log.Infof("continue pull binlog from pump %s", p.nodeID)
	}
}

// PullBinlog return the chan to get item from pump
func (p *Pump) PullBinlog(pctx context.Context, last int64) chan MergeItem {
	p.ctx, p.cancel = context.WithCancel(pctx)
	ret := make(chan MergeItem, 10)

	go func() {
		log.Debug("start PullBinlog pump: ", p.nodeID)
		defer func() {
			close(ret)
			log.Debug("PullBinlog pump leave: ", p.nodeID)
		}()

		for {
			select {
			case <-p.ctx.Done():
				return
			default:
			}

			if atomic.LoadInt32(&p.isPaused) == 1 {
				log.Debugf("[pump %s] is paused", p.nodeID)
				time.Sleep(time.Second)
				continue
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
					log.Errorf("unmarshal binlog error: %v", err)
					return
				}

				item := &binlogItem{
					binlog: binlog,
					nodeID: p.nodeID,
				}
				select {
				case ret <- item:
					last = binlog.CommitTs
					p.latestTS = binlog.CommitTs
				case <-p.ctx.Done():
				}
			}
		}
	}()

	return ret
}
