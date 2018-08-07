package drainer

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/node"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/soheilhy/cmux"
	"github.com/unrolled/render"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	waitTime                = 3 * time.Second
	maxTxnTimeout     int64 = 600
	heartbeatTTL      int64 = 60
	nodePrefix              = "drainers"
	heartbeatInterval       = 10 * time.Second
	clusterID         uint64
	pdReconnTimes     = 30
	maxMsgSize        = 1024 * 1024 * 1024

	// latestTS and latestTime is used for get approach ts
	latestTS   int64
	latestTime time.Time
)

// Server implements the gRPC interface,
// and maintains the runtime status
type Server struct {
	ID        string
	host      string
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
	isClosed  int32

	statusMu sync.RWMutex
	status   *node.Status

	latestTS   int64
	latestTime time.Time
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

	// get pd client and cluster ID
	pdCli, err := getPdClient(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	clusterID = pdCli.GetClusterID(ctx)
	// update latestTS and latestTime
	latestTS, err := util.GetTSO(pdCli)
	if err != nil {
		return nil, errors.Trace(err)
	}
	latestTime := time.Now()

	cfg.SyncerCfg.To.ClusterID = clusterID
	log.Infof("clusterID of drainer server is %v", clusterID)
	pdCli.Close()

	win := NewDepositWindow()

	cpCfg := GenCheckPointCfg(cfg, clusterID)
	cp, err := checkpoint.NewCheckPoint(cfg.SyncerCfg.DestDBType, cpCfg)
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

	advURL, err := url.Parse(cfg.ListenAddr)
	if err != nil {
		return nil, errors.Annotatef(err, "invalid configuration of advertise addr(%s)", cfg.ListenAddr)
	}

	status := node.NewStatus(ID, advURL.Host, node.Online, 0, util.GetApproachTS(latestTS, latestTime))

	return &Server{
		ID:        ID,
		host:      advURL.Host,
		cfg:       cfg,
		window:    win,
		collector: c,
		metrics:   metrics,
		tcpAddr:   cfg.ListenAddr,
		gs:        grpc.NewServer(),
		ctx:       ctx,
		cancel:    cancel,
		syncer:    syncer,
		status:    status,

		latestTS:   latestTS,
		latestTime: latestTime,
	}, nil
}

func getPdClient(cfg *Config) (pd.Client, error) {
	// lockResolver and tikvStore doesn't exposed a method to get clusterID
	// so have to create a PD client temporarily.
	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var pdCli pd.Client
	for i := 1; i < pdReconnTimes; i++ {
		pdCli, err = pd.NewClient(urlv.StringSlice(), pd.SecurityOption{
			CAPath:   cfg.Security.SSLCA,
			CertPath: cfg.Security.SSLCert,
			KeyPath:  cfg.Security.SSLKey,
		})
		if err != nil {
			time.Sleep(time.Duration(pdReconnTimes*i) * time.Millisecond)
		} else {
			break
		}
	}

	return pdCli, errors.Trace(err)
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
			if err := recover(); err != nil {
				log.Errorf("start collect panic. err: %s, stack: %s", err, debug.Stack())
			}

			log.Info("collect goroutine exited")
			s.wg.Done()
			s.Close()
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
			if err := recover(); err != nil {
				log.Errorf("start metrics panic. err: %s, stack: %s", err, debug.Stack())
			}

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
			if err := recover(); err != nil {
				log.Errorf("start syncer panic. err: %s, stack: %s", err, debug.Stack())
			}

			log.Info("syncer goroutine exited")
			s.wg.Done()
			s.Close()
		}()
		err := s.syncer.Start(jobs)
		if err != nil {
			log.Errorf("syncer exited, error %v", errors.ErrorStack(err))
		}
	}()
}

func (s *Server) heartbeat(ctx context.Context) <-chan error {
	errc := make(chan error, 1)
	err := s.updateStatus()
	if err != nil {
		errc <- errors.Trace(err)
	}

	s.wg.Add(1)
	go func() {
		defer func() {
			close(errc)
			log.Info("heartbeat goroutine exited")
			s.wg.Done()
			s.Close()
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(heartbeatInterval):
				err := s.updateStatus()
				if err != nil {
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
	err := s.updateStatus()
	if err != nil {
		return errors.Trace(err)
	}

	// start heartbeat
	errc := s.heartbeat(s.ctx)
	go func() {
		for err := range errc {
			log.Errorf("send heart error %v", err)
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

	router := mux.NewRouter()
	router.HandleFunc("/status", s.collector.Status).Methods("GET")
	router.HandleFunc("/committs", s.GetLatestTS).Methods("GET")
	router.HandleFunc("/state/{nodeID}/{action}", s.ApplyAction).Methods("PUT")
	http.Handle("/", router)
	http.Handle("/metrics", prometheus.Handler())

	go http.Serve(httpL, nil)

	if err := m.Serve(); !strings.Contains(err.Error(), "use of closed network connection") {
		return errors.Trace(err)
	}

	return nil
}

// ApplyAction change the pump's state, now can be pause or close.
func (s *Server) ApplyAction(w http.ResponseWriter, r *http.Request) {
	rd := render.New(render.Options{
		IndentJSON: true,
	})

	nodeID := mux.Vars(r)["nodeID"]
	action := mux.Vars(r)["action"]
	log.Infof("node %s receive action %s", nodeID, action)

	if nodeID != s.ID {
		rd.JSON(w, http.StatusOK, util.ErrResponsef("invalide nodeID %s, this pump's nodeID is %s", nodeID, s.ID))
		return
	}

	s.statusMu.RLock()
	if s.status.State != node.Online {
		rd.JSON(w, http.StatusOK, util.ErrResponsef("this pump's state is %s, apply %s failed!", s.status.State, action))
		s.statusMu.RUnlock()
		return
	}
	s.statusMu.RUnlock()

	s.statusMu.Lock()
	switch action {
	case "pause":
		s.status.State = node.Pausing
	case "close":
		s.status.State = node.Closing
	default:
		s.statusMu.Unlock()
		rd.JSON(w, http.StatusOK, util.ErrResponsef("invalide action %s", action))
		return
	}
	s.statusMu.Unlock()

	go s.Close()
	rd.JSON(w, http.StatusOK, util.SuccessResponse(fmt.Sprintf("apply action %s success!", action), nil))
	return
}

// GetLatestTS returns the last binlog's commit ts which synced to downstream.
func (s *Server) GetLatestTS(w http.ResponseWriter, r *http.Request) {
	rd := render.New(render.Options{
		IndentJSON: true,
	})
	ts := s.syncer.GetLatestCommitTS()
	rd.JSON(w, http.StatusOK, util.SuccessResponse("get drainer's latest ts success!", map[string]int64{"ts": ts}))
	return
}

// commitStatus commit the node's last status to pd when close the server.
func (s *Server) commitStatus() {
	// update this node
	s.statusMu.Lock()
	switch s.status.State {
	case node.Pausing, node.Online:
		s.status.State = node.Paused
	case node.Closing:
		s.status.State = node.Offline
	}
	s.statusMu.Unlock()

	err := s.updateStatus()
	if err != nil {
		log.Errorf("%s failed to update status", s.ID)
		return
	}

	log.Infof("%s has already update status ", s.ID)
}

func (s *Server) updateStatus() error {
	s.statusMu.Lock()
	s.status.UpdateTS = util.GetApproachTS(s.latestTS, s.latestTime)
	err := s.collector.reg.UpdateNode(context.Background(), nodePrefix, s.status)
	s.statusMu.Unlock()
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Close stops all goroutines started by drainer server gracefully
func (s *Server) Close() {
	log.Info("begin to close drainer server")

	if atomic.CompareAndSwapInt32(&s.isClosed, 0, 1) == false {
		log.Debug("server had closed")
		return
	}

	// notify all goroutines to exit
	s.cancel()
	// waiting for goroutines exit
	s.wg.Wait()

	// update drainer's status
	s.commitStatus()

	// stop gRPC server
	s.gs.Stop()
}
