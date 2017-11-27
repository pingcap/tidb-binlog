package drainer

import (
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/soheilhy/cmux"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var waitTime = 3 * time.Second
var maxTxnTimeout int64 = 600
var heartbeatTTL int64 = 60
var nodePrefix = "cisterns"
var heartbeatInterval = 10 * time.Second
var clusterID uint64
var maxHeapSize = 16 << 16

// Server implements the gRPC interface,
// and maintains the runtime status
type Server struct {
	ID        string
	cfg       *Config
	window    *DepositWindow
	collector *Collector
	tcpAddr   string
	gs        *grpc.Server
	metrics   *metricClient
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	syncer    *Syncer
}

func init() {
	// tracing has suspicious leak problem, so disable it here.
	// it must be set before any real grpc operation.
	grpc.EnableTracing = false
}

// NewServer return a instance of binlog-server
func NewServer(cfg *Config) (*Server, error) {
	ID, err := genDrainerID(cfg.ListenAddr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err1 := os.MkdirAll(cfg.DataDir, 0700); err1 != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	// lockResolver and tikvStore doesn't exposed a method to get clusterID
	// so have to create a PD client temporarily.
	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	pdCli, err := pd.NewClient(urlv.StringSlice())
	if err != nil {
		return nil, errors.Trace(err)
	}
	clusterID = pdCli.GetClusterID(ctx)
	log.Infof("clusterID of drainer server is %v", clusterID)
	pdCli.Close()

	win := NewDepositWindow()
	cpCfg := GenCheckPointCfg(cfg, clusterID)
	log.Infof("CheckPointCfg is %+v", cpCfg)
	cp, _ := checkpoint.NewCheckPoint(cfg.SyncerCfg.DestDBType, cpCfg)
	log.Infof("cfg.SyncerCfg.DestDBType is %+v", cfg.SyncerCfg.DestDBType)
	err = cp.Load()
	if err != nil {
		return nil, errors.Trace(err)
	}

	syncer, err := NewSyncer(ctx, cp, cfg.SyncerCfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c, err := NewCollector(cfg, clusterID, win, syncer, cp)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var metrics *metricClient
	if cfg.MetricsAddr != "" && cfg.MetricsInterval != 0 {
		metrics = &metricClient{
			addr:     cfg.MetricsAddr,
			interval: cfg.MetricsInterval,
		}
	}

	return &Server{
		ID:        ID,
		cfg:       cfg,
		window:    win,
		collector: c,
		metrics:   metrics,
		tcpAddr:   cfg.ListenAddr,
		gs:        grpc.NewServer(),
		ctx:       ctx,
		cancel:    cancel,
		syncer:    syncer,
	}, nil
}

// DumpBinlog implements the gRPC interface of drainer server
func (s *Server) DumpBinlog(req *binlog.DumpBinlogReq, stream binlog.Cistern_DumpBinlogServer) (err error) {
	return nil
}

// Notify implements the gRPC interface of drainer server
func (s *Server) Notify(ctx context.Context, in *binlog.NotifyReq) (*binlog.NotifyResp, error) {
	err := s.collector.Notify()
	if err != nil {
		log.Errorf("grpc call notify error: %v", err)
	}
	return nil, errors.Trace(err)
}

// DumpDDLJobs implements the gRPC interface of drainer server
func (s *Server) DumpDDLJobs(ctx context.Context, req *binlog.DumpDDLJobsReq) (resp *binlog.DumpDDLJobsResp, err error) {
	return
}

func calculateForwardAShortTime(current int64) int64 {
	physical := oracle.ExtractPhysical(uint64(current))
	prevPhysical := physical - int64(10*time.Minute/time.Millisecond)
	previous := oracle.ComposeTS(prevPhysical, 0)
	if previous < 0 {
		return 0
	}
	return int64(previous)
}

// StartCollect runs Collector up in a goroutine.
func (s *Server) StartCollect() {
	s.wg.Add(1)
	go func() {
		defer func() {
			log.Info("collect goroutine exited")
			s.wg.Done()
		}()
		s.collector.Start(s.ctx)
	}()
}

// StartMetrics runs a metrics colletcor in a goroutine
func (s *Server) StartMetrics() {
	if s.metrics == nil {
		return
	}
	s.wg.Add(1)
	go func() {
		defer func() {
			log.Info("metrics goroutine exited")
			s.wg.Done()
		}()
		s.metrics.Start(s.ctx, s.ID)
	}()
}

// StartSyncer runs a syncer in a goroutine
func (s *Server) StartSyncer(jobs []*model.Job) {
	s.wg.Add(1)
	go func() {
		defer func() {
			log.Info("syncer goroutine exited")
			s.wg.Done()
			s.Close()
		}()
		err := s.syncer.Start(jobs)
		if err != nil {
			log.Errorf("syncer exited, error %v", err)
		}
	}()
}

func (s *Server) heartbeat(ctx context.Context, id string) <-chan error {
	errc := make(chan error, 1)
	// must refresh node firstly
	if err := s.collector.reg.RefreshNode(ctx, nodePrefix, id, heartbeatTTL); err != nil {
		errc <- errors.Trace(err)
	}
	s.wg.Add(1)
	go func() {
		defer func() {
			close(errc)
			log.Info("heartbeat goroutine exited")
			s.wg.Done()
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(heartbeatInterval):
				if err := s.collector.reg.RefreshNode(ctx, nodePrefix, id, heartbeatTTL); err != nil {
					errc <- errors.Trace(err)
				}
			}
		}
	}()
	return errc
}

// Start runs CisternServer to serve the listening addr, and starts to collect binlog
func (s *Server) Start() error {
	// register drainer
	advURL, err := url.Parse(s.cfg.ListenAddr)
	if err != nil {
		return errors.Annotatef(err, "invalid configuration of advertise addr(%s)", s.cfg.ListenAddr)
	}
	err = s.collector.reg.RegisterNode(s.ctx, nodePrefix, s.ID, advURL.Host)
	if err != nil {
		return errors.Trace(err)
	}

	// start heartbeat
	errc := s.heartbeat(s.ctx, s.ID)
	go func() {
		for err := range errc {
			log.Error(err)
		}
	}()

	jobs, err := s.collector.LoadHistoryDDLJobs()
	if err != nil {
		return errors.Trace(err)
	}

	// start to collect
	s.StartCollect()

	// collect metrics to prometheus
	s.StartMetrics()

	// start a syncer
	s.StartSyncer(jobs)

	// start a TCP listener
	tcpURL, err := url.Parse(s.tcpAddr)
	if err != nil {
		return errors.Annotatef(err, "invalid listening tcp addr (%s)", s.tcpAddr)
	}
	tcpLis, err := net.Listen("tcp", tcpURL.Host)
	if err != nil {
		return errors.Annotatef(err, "fail to start TCP listener on %s", tcpURL.Host)
	}
	m := cmux.New(tcpLis)
	grpcL := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())

	// register drainer server with gRPC server and start to serve listener
	binlog.RegisterCisternServer(s.gs, s)
	go s.gs.Serve(grpcL)

	http.HandleFunc("/status", s.collector.Status)
	http.Handle("/metrics", prometheus.Handler())
	go http.Serve(httpL, nil)

	return m.Serve()
}

// Close stops all goroutines started by drainer server gracefully
func (s *Server) Close() {
	// unregister drainer
	if err := s.collector.reg.UnregisterNode(s.ctx, nodePrefix, s.ID); err != nil {
		log.Error(errors.ErrorStack(err))
	}
	// stop syncer
	s.syncer.Close()
	// notify all goroutines to exit
	s.cancel()
	// waiting for goroutines exit
	s.wg.Wait()
	//  stop gRPC server
	s.gs.Stop()
}
