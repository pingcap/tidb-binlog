// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package drainer

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/checkpoint"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/node"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
	"github.com/unrolled/render"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	nodePrefix        = "drainers"
	heartbeatInterval = 1 * time.Second
	getPdClient       = util.GetPdClient
)

type drainerKeyType string

// Server implements the gRPC interface,
// and maintains the runtime status
type Server struct {
	ID   string
	host string
	cfg  *Config

	collector *Collector
	tcpAddr   string
	gs        *grpc.Server
	metrics   *util.MetricClient
	ctx       context.Context
	cancel    context.CancelFunc
	tg        taskGroup
	syncer    *Syncer
	cp        checkpoint.CheckPoint
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
	if cfg.NodeID == "" {
		var err error
		cfg.NodeID, err = genDrainerID(cfg.ListenAddr)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return nil, err
	}

	if cfg.tls != nil {
		// TODO: avoid this magic enabling TLS for tikv client.
		var _ = cfg.Security.ToTiDBSecurityConfig()
	}

	// get pd client and cluster ID
	pdCli, err := getPdClient(cfg.EtcdURLs, cfg.Security)
	if err != nil {
		ferr := feedByRelayLogIfNeed(cfg)
		if ferr != nil && errors.Cause(ferr) != checkpoint.ErrNoCheckpointItem {
			return nil, errors.Trace(ferr)
		}
		return nil, errors.Trace(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, drainerKeyType("compressor"), cfg.Compressor)

	clusterID := pdCli.GetClusterID(ctx)
	log.Info("get cluster id from pd", zap.Uint64("id", clusterID))
	// update latestTS and latestTime
	latestTS, err := util.GetTSO(pdCli)
	if err != nil {
		return nil, errors.Trace(err)
	}
	latestTime := time.Now()

	if cfg.InitialCommitTS == -1 {
		log.Info("set InitialCommitTS", zap.Int64("ts", latestTS))
		cfg.InitialCommitTS = latestTS
	}

	cfg.SyncerCfg.To.ClusterID = clusterID
	pdCli.Close()

	cpCfg, err := GenCheckPointCfg(cfg, clusterID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cp, err := checkpoint.NewCheckPoint(cpCfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	checkpointTSOGauge.Set(float64(oracle.ExtractPhysical(uint64(cp.TS()))))

	syncer, err := createSyncer(cfg.EtcdURLs, cp, cfg.SyncerCfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c, err := NewCollector(cfg, clusterID, syncer, cp)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var metrics *util.MetricClient
	if cfg.MetricsAddr != "" && cfg.MetricsInterval != 0 {
		metrics = util.NewMetricClient(
			cfg.MetricsAddr,
			time.Duration(cfg.MetricsInterval)*time.Second,
			registry,
		)
	}

	advURL, err := url.Parse(cfg.AdvertiseAddr)
	if err != nil {
		return nil, errors.Annotatef(err, "invalid configuration of advertise addr(%s)", cfg.AdvertiseAddr)
	}

	status := node.NewStatus(cfg.NodeID, advURL.Host, node.Online, 0, syncer.GetLatestCommitTS(), util.GetApproachTS(latestTS, latestTime))

	return &Server{
		ID:        cfg.NodeID,
		host:      advURL.Host,
		cfg:       cfg,
		collector: c,
		metrics:   metrics,
		tcpAddr:   cfg.ListenAddr,
		gs:        grpc.NewServer(),
		ctx:       ctx,
		cancel:    cancel,
		syncer:    syncer,
		cp:        cp,
		status:    status,

		latestTS:   latestTS,
		latestTime: latestTime,
	}, nil
}

func createSyncer(etcdURLs string, cp checkpoint.CheckPoint, cfg *SyncerConfig) (syncer *Syncer, err error) {
	tiStore, err := createTiStore(etcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer tiStore.Close()

	jobs, err := loadHistoryDDLJobs(tiStore)
	if err != nil {
		return nil, errors.Trace(err)
	}

	syncer, err = NewSyncer(cp, cfg, jobs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return
}

// DumpBinlog implements the gRPC interface of drainer server
func (s *Server) DumpBinlog(req *binlog.DumpBinlogReq, stream binlog.Cistern_DumpBinlogServer) (err error) {
	return nil
}

// Notify implements the gRPC interface of drainer server
func (s *Server) Notify(ctx context.Context, in *binlog.NotifyReq) (*binlog.NotifyResp, error) {
	log.Debug("recv Notify")

	err := s.collector.Notify()
	if err != nil {
		log.Error("grpc call notify failed", zap.Error(err))
	}
	return nil, errors.Trace(err)
}

// DumpDDLJobs implements the gRPC interface of drainer server
func (s *Server) DumpDDLJobs(ctx context.Context, req *binlog.DumpDDLJobsReq) (resp *binlog.DumpDDLJobsResp, err error) {
	return
}

func (s *Server) heartbeat(ctx context.Context) <-chan error {
	errc := make(chan error, 1)

	s.tg.Go("heartbeat", func() {
		defer func() {
			close(errc)
			go s.Close()
		}()

		for {
			err := s.updateStatus()
			if err != nil {
				errc <- errors.Trace(err)
			}
			select {
			case <-time.After(heartbeatInterval):
			case <-ctx.Done():
				return
			}
		}
	})
	return errc
}

// Start runs CisternServer to serve the listening addr, and starts to collect binlog
func (s *Server) Start() error {
	// register drainer
	if err := s.updateStatus(); err != nil {
		return errors.Trace(err)
	}
	log.Info("register success", zap.String("drainer node id", s.ID))

	// start heartbeat
	errc := s.heartbeat(s.ctx)
	go func() {
		for err := range errc {
			log.Error("send heart failed", zap.Error(err))
		}
	}()

	s.tg.GoNoPanic("collect", func() {
		defer func() { go s.Close() }()
		s.collector.Start(s.ctx)
	})

	if s.metrics != nil {
		s.tg.GoNoPanic("metrics", func() {
			s.metrics.Start(s.ctx, map[string]string{"instance": s.ID})
		})
	}

	s.tg.GoNoPanic("syncer", func() {
		defer func() { go s.Close() }()
		if err := s.syncer.Start(); err != nil {
			log.Error("syncer exited abnormal", zap.Error(err))
		}
	})

	// We need to manage TLS here for cmux to distinguish between HTTP and gRPC.
	tcpLis, err := util.Listen("tcp", s.tcpAddr, s.cfg.tls)
	if err != nil {
		return errors.Trace(err)
	}

	m := cmux.New(tcpLis)
	grpcL := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())

	// register drainer server with gRPC server and start to serve listener
	binlog.RegisterCisternServer(s.gs, s)
	go s.gs.Serve(grpcL)

	router := s.initAPIRouter()
	http.Handle("/", router)

	go http.Serve(httpL, nil)

	log.Info("start to server request", zap.String("addr", s.tcpAddr))
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

	vars := mux.Vars(r)
	nodeID, action := vars["nodeID"], vars["action"]
	log.Info("receive apply action request", zap.String("nodeID", nodeID), zap.String("action", action))

	if nodeID != s.ID {
		rd.JSON(w, http.StatusOK, util.ErrResponsef("invalid nodeID %s, this pump's nodeID is %s", nodeID, s.ID))
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
		rd.JSON(w, http.StatusOK, util.ErrResponsef("invalid action %s", action))
		return
	}
	s.statusMu.Unlock()

	go s.Close()
	rd.JSON(w, http.StatusOK, util.SuccessResponse(fmt.Sprintf("apply action %s success!", action), nil))
}

// GetLatestTS returns the last binlog's commit ts which synced to downstream.
func (s *Server) GetLatestTS(w http.ResponseWriter, r *http.Request) {
	rd := render.New(render.Options{
		IndentJSON: true,
	})
	ts := s.syncer.GetLatestCommitTS()
	rd.JSON(w, http.StatusOK, util.SuccessResponse("get drainer's latest ts success!", map[string]int64{"ts": ts}))
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
		log.Error("update status failed", zap.String("id", s.ID))
		return
	}

	log.Info("has already update status", zap.String("id", s.ID))
}

func (s *Server) updateStatus() error {
	s.statusMu.Lock()
	s.status.UpdateTS = util.GetApproachTS(s.latestTS, s.latestTime)
	s.status.MaxCommitTS = s.syncer.GetLatestCommitTS()
	status := node.CloneStatus(s.status)
	s.statusMu.Unlock()

	err := s.collector.reg.UpdateNode(context.Background(), nodePrefix, status)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (s *Server) initAPIRouter() *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/status", s.collector.Status).Methods("GET")
	router.HandleFunc("/commit_ts", s.GetLatestTS).Methods("GET")
	router.HandleFunc("/state/{nodeID}/{action}", s.ApplyAction).Methods("PUT")
	prometheus.DefaultGatherer = registry
	router.Handle("/metrics", promhttp.Handler())
	return router
}

// Close stops all goroutines started by drainer server gracefully
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt32(&s.isClosed, 0, 1) {
		log.Debug("server had closed")
		return
	}

	log.Info("begin to close drainer server")

	// update drainer's status
	s.commitStatus()
	log.Info("commit status done")

	// notify all goroutines to exit
	s.cancel()
	s.syncer.Close()
	// waiting for goroutines exit
	s.tg.Wait()
	// close the CheckPoint
	err := s.cp.Close()
	if err != nil {
		log.Error("close checkpoint failed", zap.Error(err))
	}

	// stop gRPC server
	s.gs.Stop()
	log.Info("drainer exit")
}

func createTiStore(urls string) (kv.Storage, error) {
	urlv, err := flags.NewURLsValue(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := store.Register("tikv", tikv.Driver{}); err != nil {
		return nil, errors.Trace(err)
	}
	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	tiStore, err := store.New(tiPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return tiStore, nil
}
