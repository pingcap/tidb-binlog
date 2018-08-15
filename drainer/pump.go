package drainer

import (
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pump"
	pb "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	receiveBinlogRetryTime = 10
	binlogChanSize         = 10
)

// Pump holds the connection to a pump node, and keeps the savepoint of binlog last read
type Pump struct {
	nodeID    string
	addr      string
	clusterID uint64
	// the latest binlog ts that pump had handled
	latestTS int64

	isClosed int32

	isPaused int32

	errCh chan error
}

// NewPump returns an instance of Pump
func NewPump(nodeID, addr string, clusterID uint64, startTs int64, errCh chan error) *Pump {
	nodeID, err := pump.FormatNodeID(nodeID)
	if err != nil {
		log.Warnf("[pump %s] node id maybe illegal", nodeID)
	}

	return &Pump{
		nodeID:    nodeID,
		addr:      addr,
		clusterID: clusterID,
		latestTS:  startTs,
		errCh:     errCh,
	}
}

// Close sets isClose to 1, and pull binlog will be exit.
func (p *Pump) Close() {
	log.Infof("[pump %s] is closing", p.nodeID)
	atomic.StoreInt32(&p.isClosed, 1)
}

// Pause sets isPaused to 1, and stop pull binlog from pump. This function is reentrant.
func (p *Pump) Pause() {
	// use CompareAndSwapInt32 to avoid redundant log
	if atomic.CompareAndSwapInt32(&p.isPaused, 0, 1) {
		log.Infof("[pump %s] pause pull binlog", p.nodeID)
	}
}

// Continue sets isPaused to 0, and continue pull binlog from pump. This function is reentrant.
func (p *Pump) Continue() {
	// use CompareAndSwapInt32 to avoid redundant log
	if atomic.CompareAndSwapInt32(&p.isPaused, 1, 0) {
		log.Infof("[pump %s] continue pull binlog", p.nodeID)
	}
}

// PullBinlog returns the chan to get item from pump
func (p *Pump) PullBinlog(pctx context.Context, last int64) chan MergeItem {
	ret := make(chan MergeItem, binlogChanSize)

	go func() {
		log.Debugf("[pump %s] start PullBinlog", p.nodeID)

		pullCli, grpcConn, err := p.createPullBinlogsClient(pctx, last)
		if err != nil {
			p.reportErr(pctx, err)
			return
		}

		defer func() {
			close(ret)
			if grpcConn != nil {
				grpcConn.Close()
			}
			log.Debugf("[pump %s] stop PullBinlog", p.nodeID)
		}()

		for {
			if atomic.LoadInt32(&p.isClosed) == 1 {
				return
			}

			if atomic.LoadInt32(&p.isPaused) == 1 {
				// this pump is paused, wait until it can pull binlog again
				log.Debugf("[pump %s] is paused", p.nodeID)
				time.Sleep(time.Second)
				continue
			}

			resp, err := pullCli.Recv()
			if err != nil {
				log.Errorf("[pump %s] receive binlog error %v", p.nodeID, err)
				time.Sleep(time.Second)
				// TODO: add metric here
				continue
			}

			binlog := new(pb.Binlog)
			err = binlog.Unmarshal(resp.Entity.Payload)
			if err != nil {
				log.Errorf("[pump %s] unmarshal binlog error: %v", p.nodeID, err)
				p.reportErr(pctx, err)
				return
			}

			item := &binlogItem{
				binlog: binlog,
				nodeID: p.nodeID,
			}
			select {
			case ret <- item:
				if binlog.CommitTs > last {
					last = binlog.CommitTs
					p.latestTS = binlog.CommitTs
				} else {
					log.Errorf("[pump %s] receive unsort binlog", p.nodeID)
				}
			case <-pctx.Done():
				return
			}
		}
	}()

	return ret
}

func (p *Pump) createPullBinlogsClient(ctx context.Context, last int64) (pb.Pump_PullBinlogsClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(p.addr, grpc.WithInsecure())
	if err != nil {
		log.Errorf("[pump %s] create grpc dial error %v", p.nodeID, err)
		return nil, nil, errors.Trace(err)
	}

	cli := pb.NewPumpClient(conn)

	in := &pb.PullBinlogReq{
		ClusterID: p.clusterID,
		StartFrom: pb.Pos{Offset: last},
	}
	pullCli, err := cli.PullBinlogs(ctx, in)
	if err != nil {
		log.Error("[pump %s] create PullBinlogs client error %v", p.nodeID, err)
		return nil, nil, errors.Trace(err)
	}

	return pullCli, conn, nil
}

func (p *Pump) reportErr(ctx context.Context, err error) {
	select {
	case <-ctx.Done():
		return
	case p.errCh <- err:
		return
	}
}
