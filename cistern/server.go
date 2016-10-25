package cistern

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var windowNamespace []byte
var binlogNamespace []byte
var savepointNamespace []byte
var ddlJobNamespace []byte

// Server implements the gRPC interface,
// and maintains the runtime status
type Server struct {
	boltdb    store.Store
	window    *DepositWindow
	collector *Collector
	publisher *Publisher
	tcpAddr   string
	gs        *grpc.Server
	metrics   *metricClient
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	gc        time.Duration
}

// NewServer return a instance of binlog-server
func NewServer(cfg *Config) (*Server, error) {
	windowNamespace = []byte(fmt.Sprintf("window_%d", cfg.ClusterID))
	binlogNamespace = []byte(fmt.Sprintf("binlog_%d", cfg.ClusterID))
	savepointNamespace = []byte(fmt.Sprintf("savepoint_%d", cfg.ClusterID))
	ddlJobNamespace = []byte(fmt.Sprintf("ddljob_%d", cfg.ClusterID))

	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return nil, err
	}

	s, err := store.NewBoltStore(path.Join(cfg.DataDir, "data.bolt"), [][]byte{
		windowNamespace,
		binlogNamespace,
		savepointNamespace,
		ddlJobNamespace,
	})
	if err != nil {
		return nil, errors.Annotatef(err, "failed to open BoltDB store in dir(%s)", cfg.DataDir)
	}

	win, err := NewDepositWindow(s)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c, err := NewCollector(cfg, s, win)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err := c.LoadHistoryDDLJobs(); err != nil {
		return nil, errors.Trace(err)
	}

	p := NewPublisher(cfg, s, win)

	ctx, cancel := context.WithCancel(context.Background())

	var metrics *metricClient
	if cfg.MetricsAddr != "" && cfg.MetricsInterval != 0 {
		metrics = &metricClient{
			addr:     cfg.MetricsAddr,
			interval: cfg.MetricsInterval,
		}
	}

	var gc time.Duration
	if cfg.GC > 0 {
		gc = time.Duration(cfg.GC) * 24 * time.Hour
	}

	return &Server{
		boltdb:    s,
		window:    win,
		collector: c,
		publisher: p,
		metrics:   metrics,
		tcpAddr:   cfg.ListenAddr,
		gs:        grpc.NewServer(),
		ctx:       ctx,
		cancel:    cancel,
		gc:        gc,
	}, nil
}

// DumpBinlog implements the gRPC interface of cistern server
func (s *Server) DumpBinlog(req *binlog.DumpBinlogReq, stream binlog.Cistern_DumpBinlogServer) error {
	batch := 1000
	latest := req.BeginCommitTS

	for {
		end := s.window.LoadLower()
		if latest >= end {
			time.Sleep(1 * time.Second)
			continue
		}

		var resps []*binlog.DumpBinlogResp
		err := s.boltdb.Scan(
			binlogNamespace,
			codec.EncodeInt([]byte{}, latest),
			func(key []byte, val []byte) (bool, error) {
				_, cts, err := codec.DecodeInt(key)
				if err != nil {
					return false, errors.Trace(err)
				}
				if cts > end || len(resps) >= batch {
					return false, nil
				}
				if cts == latest {
					return true, nil
				}
				payload, _, err := decodePayload(val)
				if err != nil {
					return false, errors.Trace(err)
				}
				ret := &binlog.DumpBinlogResp{
					CommitTS: cts,
					Payload:  payload,
				}
				resps = append(resps, ret)
				return true, nil
			},
		)
		if err != nil {
			return errors.Trace(err)
		}

		for _, resp := range resps {
			item := &binlog.Binlog{}
			if err := item.Unmarshal(resp.Payload); err != nil {
				return errors.Trace(err)
			}
			if item.DdlJobId > 0 {
				key := codec.EncodeInt([]byte{}, item.DdlJobId)
				data, err := s.boltdb.Get(ddlJobNamespace, key)
				if err != nil {
					return errors.Annotatef(err,
						"DDL Job(%d) not found, with binlog commitTS(%d)", item.DdlJobId, resp.CommitTS)
				}
				resp.Ddljob = data
			}
			if err := stream.Send(resp); err != nil {
				return errors.Trace(err)
			}
			latest = resp.CommitTS
		}
	}
}

// DumpDDLJobs implements the gRPC interface of cistern server
func (s *Server) DumpDDLJobs(ctx context.Context, req *binlog.DumpDDLJobsReq) (*binlog.DumpDDLJobsResp, error) {
	return nil, errors.New("implement in next pr")
}

// StartCollect runs Collector up in a goroutine.
func (s *Server) StartCollect() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.collector.Start(s.ctx)
	}()
}

// StartPublish runs Publisher up in a goroutine.
func (s *Server) StartPublish() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.publisher.Start(s.ctx)
	}()
}

// StartMetrics runs a metrics colletcor in a goroutine
func (s *Server) StartMetrics() {
	if s.metrics == nil {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.metrics.Start(s.ctx)
	}()
}

// StartGC runs GC periodically in a goroutine.
func (s *Server) StartGC() {
	if s.gc == 0 {
		return
	}
	s.wg.Add(1)
	go func() {
		timer := time.NewTicker(time.Hour)
		defer s.wg.Done()
		defer timer.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-timer.C:
				err := GC(s.boltdb, binlogNamespace, s.gc)
				if err != nil {
					log.Error("GC binlog error:", errors.ErrorStack(err))
				}
			}
		}
	}()
}

// Start runs CisternServer to serve the listening addr, and starts to collect binlog
func (s *Server) Start() error {
	// start to collect
	s.StartCollect()

	// start to publish
	s.StartPublish()

	// collect metrics to prometheus
	s.StartMetrics()

	// recycle old binlog
	s.StartGC()

	// start a TCP listener
	tcpURL, err := url.Parse(s.tcpAddr)
	if err != nil {
		return errors.Annotatef(err, "invalid listening tcp addr (%s)", s.tcpAddr)
	}
	tcpLis, err := net.Listen("tcp", tcpURL.Host)
	if err != nil {
		return errors.Annotatef(err, "fail to start TCP listener on %s", tcpURL.Host)
	}

	// register cistern server with gRPC server and start to serve listener
	binlog.RegisterCisternServer(s.gs, s)
	s.gs.Serve(tcpLis)
	return nil
}

// Close stops all goroutines started by cistern server gracefully
func (s *Server) Close() {
	// first stop gRPC server
	s.gs.Stop()
	// notify all goroutines to exit
	s.cancel()
	// waiting for goroutines exit
	s.wg.Wait()
	s.boltdb.Close()
}
