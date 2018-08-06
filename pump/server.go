package pump

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pump/storage"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tipb/go-binlog"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/soheilhy/cmux"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var pullBinlogInterval = 50 * time.Millisecond

// GlobalConfig is global config of pump
var GlobalConfig *globalConfig

const (
	slowDist      = 30 * time.Millisecond
	mib           = 1024 * 1024
	pdReconnTimes = 30
)

// use latestPos and latestTS to record the latest binlog position and ts the pump works on
var (
	latestKafkaPos binlog.Pos
	latestFilePos  binlog.Pos
	latestTS       int64
)

// Server implements the gRPC interface,
// and maintains pump's status at run time.
type Server struct {
	dataDir string
	storage storage.Storage

	clusterID string

	// node maintains the status of this pump and interact with etcd registry
	node Node

	tcpAddr                 string
	unixAddr                string
	gs                      *grpc.Server
	ctx                     context.Context
	cancel                  context.CancelFunc
	wg                      sync.WaitGroup
	gc                      time.Duration
	metrics                 *metricClient
	lastWriteBinlogUnixNano int64
	needGenBinlog           AtomicBool
	pdCli                   pd.Client
	cfg                     *Config

	cp *checkPoint
}

func init() {
	// tracing has suspicious leak problem, so disable it here.
	// it must be set before any real grpc operation.
	grpc.EnableTracing = false
	GlobalConfig = &globalConfig{
		maxMsgSize:        defautMaxKafkaSize,
		segmentSizeBytes:  defaultSegmentSizeBytes,
		SlicesSize:        defaultBinlogSliceSize,
		sendKafKaRetryNum: defaultSendKafKaRetryNum,
	}
}

// NewServer returns a instance of pump server
func NewServer(cfg *Config) (*Server, error) {
	n, err := NewPumpNode(cfg)
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

	// get pd client and cluster ID
	pdCli, err := getPdClient(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	clusterID := pdCli.GetClusterID(ctx)
	log.Infof("clusterID of pump server is %v", clusterID)

	grpcOpts := []grpc.ServerOption{grpc.MaxMsgSize(GlobalConfig.maxMsgSize)}
	if cfg.tls != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(cfg.tls)))
	}

	urlv, err := flags.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	lockResolver, err := tikv.NewLockResolver(urlv.StringSlice(), cfg.Security.ToTiDBSecurityConfig())
	if err != nil {
		return nil, errors.Trace(err)
	}

	session.RegisterStore("tikv", tikv.Driver{})
	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	tiStore, err := session.NewStore(tiPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	storage, err := storage.NewAppendWithResolver(cfg.DataDir, nil, tiStore, lockResolver)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Server{
		dataDir:   cfg.DataDir,
		storage:   storage,
		clusterID: fmt.Sprintf("%d", clusterID),
		node:      n,
		tcpAddr:   cfg.ListenAddr,
		unixAddr:  cfg.Socket,
		gs:        grpc.NewServer(grpcOpts...),
		ctx:       ctx,
		cancel:    cancel,
		metrics:   metrics,
		gc:        time.Duration(cfg.GC) * 24 * time.Hour,
		pdCli:     pdCli,
		cfg:       cfg,
	}, nil
}

func getPdClient(cfg *Config) (pd.Client, error) {
	// use tiStore's currentVersion method to get the ts from tso
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

func (s *Server) init() error {

	return nil
}

// WriteBinlog implements the gRPC interface of pump server
func (s *Server) WriteBinlog(ctx context.Context, in *binlog.WriteBinlogReq) (*binlog.WriteBinlogResp, error) {
	var err error
	beginTime := time.Now()
	atomic.StoreInt64(&s.lastWriteBinlogUnixNano, beginTime.UnixNano())

	defer func() {
		var label string
		if err != nil {
			label = "fail"
		} else {
			label = "succ"
		}

		rpcHistogram.WithLabelValues("WriteBinlog", label).Observe(time.Since(beginTime).Seconds())
	}()

	cid := fmt.Sprintf("%d", in.ClusterID)
	if cid != s.clusterID {
		err = errors.Errorf("cluster ID are mismatch, %v vs %v", cid, s.clusterID)
		return nil, err
	}

	ret := new(binlog.WriteBinlogResp)

	blog := new(binlog.Binlog)
	err = blog.Unmarshal(in.Payload)
	if err != nil {
		goto errHandle
	}

	err = s.storage.WriteBinlog(blog)
	if err != nil {
		goto errHandle
	}

	return ret, nil

errHandle:
	lossBinlogCacheCounter.Add(1)
	log.Errorf("write binlog error %v in %s mode", err, s.cfg.WriteMode)
	if !s.cfg.EnableTolerant {
		ret.Errmsg = err.Error()
		return ret, err
	}

	return ret, nil
}

// PullBinlogs sends binlogs in the streaming way
func (s *Server) PullBinlogs(in *binlog.PullBinlogReq, stream binlog.Pump_PullBinlogsServer) error {
	log.Debug("get PullBinlogs req: ", in)
	defer func() {
		log.Debug("PullBinlogs req: ", in, " quit")
	}()

	cid := fmt.Sprintf("%d", in.ClusterID)
	if cid != s.clusterID {
		return errors.Errorf("cluster ID are mismatch, %v vs %v", cid, s.clusterID)
	}

	// don't use pos.Suffix now, use offset like last commitTS
	last := in.StartFrom.Offset

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	binlogs := s.storage.PullCommitBinlog(ctx, last)

	for {
		select {
		case data, ok := <-binlogs:
			if !ok {
				return nil
			}
			resp := new(binlog.PullBinlogResp)
			blog := new(binlog.Binlog)
			err := blog.Unmarshal(data)
			if err != nil {
				log.Error(err)
				continue
			}
			log.Debug("PullBinlogs get commitTS: ", blog.CommitTs)

			resp.Entity.Pos.Offset = blog.CommitTs
			resp.Entity.Payload = data
			err = stream.Send(resp)
			if err != nil {
				return err
			}
			log.Debug("PullBinlogs send binlog commitTS: ", blog.CommitTs, "success")
		case <-s.ctx.Done():
			return nil
		}
	}
}

// Start runs Pump Server to serve the listening addr, and maintains heartbeat to Etcd
func (s *Server) Start() error {
	// register this node
	if err := s.node.Register(s.ctx); err != nil {
		return errors.Annotate(err, "fail to register node to etcd")
	}

	log.Debug("register success")

	// notify all cisterns
	if err := s.node.Notify(s.ctx); err != nil {
		// if fail, unregister this node
		if err := s.node.Unregister(context.Background()); err != nil {
			log.Errorf("unregister pump while pump fails to notify drainer error %v", errors.ErrorStack(err))
		}
		return errors.Annotate(err, "fail to notify all living drainer")
	}

	log.Debug("notify success")

	errc := s.node.Heartbeat(s.ctx)
	go func() {
		for err := range errc {
			if err != context.Canceled {
				log.Errorf("send heartbeat error %v", err)
			}
		}
	}()

	// init the server
	if err := s.init(); err != nil {
		return errors.Annotate(err, "fail to initialize pump server")
	}

	log.Debug("init success")

	// start a TCP listener
	tcpURL, err := url.Parse(s.tcpAddr)
	if err != nil {
		return errors.Annotatef(err, "invalid listening tcp addr (%s)", s.tcpAddr)
	}
	tcpLis, err := net.Listen("tcp", tcpURL.Host)
	if err != nil {
		return errors.Annotatef(err, "fail to start TCP listener on %s", tcpURL.Host)
	}

	// start a UNIX listener
	unixURL, err := url.Parse(s.unixAddr)
	if err != nil {
		return errors.Annotatef(err, "invalid listening socket addr (%s)", s.unixAddr)
	}
	unixLis, err := net.Listen("unix", unixURL.Path)
	if err != nil {
		return errors.Annotatef(err, "fail to start UNIX listener on %s", unixURL.Path)
	}
	// start generate binlog if pump doesn't receive new binlogs
	s.wg.Add(1)
	go s.genForwardBinlog()

	// gc old binlog files
	s.wg.Add(1)
	go s.gcBinlogFile()

	// collect metrics to prometheus
	s.wg.Add(1)
	go s.startMetrics()

	// register pump with gRPC server and start to serve listeners
	binlog.RegisterPumpServer(s.gs, s)
	go s.gs.Serve(unixLis)

	// grpc and http will use the same tcp connection
	m := cmux.New(tcpLis)
	grpcL := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())
	go s.gs.Serve(grpcL)

	http.HandleFunc("/status", s.Status)
	http.HandleFunc("/drainers", s.AllDrainers)
	http.Handle("/metrics", prometheus.Handler())
	go http.Serve(httpL, nil)

	log.Debug("start to server request")
	err = m.Serve()
	if strings.Contains(err.Error(), "use of closed network connection") {
		err = nil
	}

	return err
}

// gennerate rollback binlog can forward the drainer's latestCommitTs, and just be discarded without any side effects
func (s *Server) genFakeBinlog() ([]byte, error) {
	ts, err := s.getTSO()
	if err != nil {
		return nil, errors.Trace(err)
	}

	bl := &binlog.Binlog{
		StartTs:  ts,
		Tp:       binlog.BinlogType_Rollback,
		CommitTs: ts,
	}
	payload, err := bl.Marshal()
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (s *Server) writeFakeBinlog() {
	payload, err := s.genFakeBinlog()
	if err != nil {
		log.Error("gennerate fake binlog err: ", err)
		return
	}

	req := new(pb.WriteBinlogReq)
	req.Payload = payload

	req.ClusterID, err = strconv.ParseUint(s.clusterID, 10, 64)
	if err != nil {
		panic("unreachable wrong clusterID: " + s.clusterID)
	}

	resp, err := s.WriteBinlog(s.ctx, req)

	if err != nil {
		log.Error("write fake binlog err: ", err)
		return
	}

	if len(resp.Errmsg) > 0 {
		log.Error("write fake binlog err: ", resp.Errmsg)
		return
	}

	log.Debug("write fake binlog successful")
	return
}

// we would generate binlog to forward the pump's latestCommitTs in drainer when there is no binlogs in this pump
func (s *Server) genForwardBinlog() {
	defer s.wg.Done()

	genFakeBinlogInterval := time.Duration(s.cfg.GenFakeBinlogInterval) * time.Second
	lastWriteBinlogUnixNano := atomic.LoadInt64(&s.lastWriteBinlogUnixNano)
	for {
		select {
		case <-s.ctx.Done():
			log.Info("genFakeBinlog exit")
			return
		case <-time.After(genFakeBinlogInterval):
			// if no WriteBinlogReq, we write a fake binlog
			if lastWriteBinlogUnixNano == atomic.LoadInt64(&s.lastWriteBinlogUnixNano) {
				s.writeFakeBinlog()
			}
			lastWriteBinlogUnixNano = atomic.LoadInt64(&s.lastWriteBinlogUnixNano)
		}
	}
}

func (s *Server) gcBinlogFile() {
	defer s.wg.Done()
	if s.gc == 0 {
		return
	}

	for {
		select {
		case <-s.ctx.Done():
			log.Info("gcBinlogFile exit")
			return
		case <-time.Tick(time.Hour):
			if s.gc == 0 {
				continue
			}
			// TODO check safe to gc for drainer
			millisecond := time.Now().Add(-time.Hour*s.gc).UnixNano() / 1000
			gcTS := int64(oracle.EncodeTSO(millisecond))
			s.storage.GCTS(gcTS)
		}
	}
}

func (s *Server) startMetrics() {
	defer s.wg.Done()

	if s.metrics == nil {
		return
	}
	log.Info("start metricClient")
	s.metrics.Start(s.ctx, s.node.ID())
	log.Info("startMetrics exit")
}

// AllDrainers exposes drainers' status to HTTP handler.
func (s *Server) AllDrainers(w http.ResponseWriter, r *http.Request) {
	node, ok := s.node.(*pumpNode)
	if !ok {
		json.NewEncoder(w).Encode("can't provide service")
		return
	}

	pumps, err := node.EtcdRegistry.Nodes(s.ctx, "cisterns")
	if err != nil {
		log.Errorf("get pumps error %v", err)
	}

	json.NewEncoder(w).Encode(pumps)
}

// Status exposes pumps' status to HTTP handler.
func (s *Server) Status(w http.ResponseWriter, r *http.Request) {
	s.PumpStatus().Status(w, r)
}

// PumpStatus returns all pumps' status.
func (s *Server) PumpStatus() *HTTPStatus {
	status, err := s.node.NodesStatus(s.ctx)
	if err != nil {
		log.Errorf("get pumps' status error %v", err)
		return &HTTPStatus{
			ErrMsg: err.Error(),
		}
	}

	// get all pumps' latest binlog position
	binlogPos := make(map[string]*LatestPos)
	for _, st := range status {
		binlogPos[st.NodeID] = &LatestPos{
			FilePos:  st.LatestFilePos,
			KafkaPos: st.LatestKafkaPos,
		}
	}
	// get ts from pd
	commitTS, err := s.getTSO()
	if err != nil {
		log.Errorf("get ts from pd, error %v", err)
		return &HTTPStatus{
			ErrMsg: err.Error(),
		}
	}

	return &HTTPStatus{
		BinlogPos:  binlogPos,
		CommitTS:   commitTS,
		CheckPoint: s.cp.pos(),
	}
}

func (s *Server) getTSO() (int64, error) {
	now := time.Now()
	physical, logical, err := s.pdCli.GetTS(context.Background())
	if err != nil {
		return 0, errors.Trace(err)
	}
	dist := time.Since(now)
	if dist > slowDist {
		log.Warnf("get timestamp too slow: %s", dist)
	}

	ts := int64(composeTS(physical, logical))
	// update latestTS by the way
	latestTS = ts

	return ts, nil
}

// Close gracefully releases resource of pump server
func (s *Server) Close() {
	// stop the gRPC server
	s.gs.GracefulStop()
	log.Info("grpc is stopped")

	// update latest for offline ts in unregister process
	if _, err := s.getTSO(); err != nil {
		log.Errorf("get tso in close error %v", errors.ErrorStack(err))
	}

	// notify other goroutines to exit
	s.cancel()
	s.wg.Wait()
	log.Info("background goroutins are stopped")

	// unregister this node
	if err := s.node.Unregister(context.Background()); err != nil {
		log.Errorf("unregister pump error %v", errors.ErrorStack(err))
	}
	log.Info(s.node.ID, " has unregister")
	// close tiStore
	if s.pdCli != nil {
		s.pdCli.Close()
	}
	log.Info("has closed pdCli")

	s.storage.Close()
}
