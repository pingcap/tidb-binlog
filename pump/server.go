package pump

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/node"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pump/storage"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tipb/go-binlog"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/soheilhy/cmux"
	"github.com/unrolled/render"
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

	clusterID uint64

	// node maintains the status of this pump and interact with etcd registry
	node node.Node

	tcpAddr  string
	unixAddr string
	gs       *grpc.Server
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	gc       time.Duration
	metrics  *metricClient
	// save the last time we write binlog to Storage
	// if long time not write, we can write a fake binlog
	lastWriteBinlogUnixNano int64
	needGenBinlog           AtomicBool
	pdCli                   pd.Client
	cfg                     *Config

	cp *checkPoint

	isClosed int32
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
		clusterID: clusterID,
		node:      n,
		unixAddr:  cfg.Socket,
		tcpAddr:   cfg.ListenAddr,
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

	if in.ClusterID != s.clusterID {
		err = errors.Errorf("cluster ID are mismatch, %v vs %v", in.ClusterID, s.clusterID)
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
	//log.Errorf("write binlog error %v in %s mode", err, s.cfg.WriteMode)
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

	if in.ClusterID != s.clusterID {
		return errors.Errorf("cluster ID are mismatch, %v vs %v", in.ClusterID, s.clusterID)
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

			resp.Entity.Payload = data
			err := stream.Send(resp)
			if err != nil {
				return err
			}
			log.Debug("PullBinlogs send binlog payload len: ", len(data), "success")
		case <-s.ctx.Done():
			return nil
		}
	}
}

// Start runs Pump Server to serve the listening addr, and maintains heartbeat to Etcd
func (s *Server) Start() error {
	// register this node
	ts, err := s.getTSO()
	if err != nil {
		return errors.Annotate(err, "fail to get tso from pd")
	}
	status := node.NewStatus(s.node.NodeStatus().NodeID, s.node.NodeStatus().Addr, node.Online, 0, ts)
	err = s.node.RefreshStatus(context.Background(), status)
	if err != nil {
		return errors.Annotate(err, "fail to register node to etcd")
	}

	log.Debug("register success")

	// notify all cisterns
	if err := s.node.Notify(s.ctx); err != nil {
		// if fail, refresh this node's state to paused
		status := node.NewStatus(s.node.NodeStatus().NodeID, s.node.NodeStatus().Addr, node.Paused, 0, 0)
		err = s.node.RefreshStatus(context.Background(), status)
		if err != nil {
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

	// start a UNIX listener
	var unixLis net.Listener
	if s.unixAddr != "" {
		unixURL, err := url.Parse(s.unixAddr)
		if err != nil {
			return errors.Annotatef(err, "invalid listening socket addr (%s)", s.unixAddr)
		}
		unixLis, err = net.Listen("unix", unixURL.Path)
		if err != nil {
			return errors.Annotatef(err, "fail to start UNIX on %s", unixURL.Path)
		}
	}

	log.Debug("init success")

	// start a TCP listener
	tcpURL, err := url.Parse(s.tcpAddr)
	if err != nil {
		return errors.Annotatef(err, "invalid listening tcp addr (%s)", s.tcpAddr)
	}
	// ugly code
	host := fmt.Sprintf("0.0.0.0:%s", strings.Split(tcpURL.Host, ":")[1])
	tcpLis, err := net.Listen("tcp", host)
	if err != nil {
		return errors.Annotatef(err, "fail to start TCP listener on %s", tcpURL.Host)
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

	if s.unixAddr != "" {
		go s.gs.Serve(unixLis)
	}

	// grpc and http will use the same tcp connection
	m := cmux.New(tcpLis)
	grpcL := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())
	go s.gs.Serve(grpcL)

	router := mux.NewRouter()
	router.HandleFunc("/status", s.Status).Methods("GET")
	router.HandleFunc("/state/{nodeID}/{action}", s.ApplyAction).Methods("PUT")
	router.HandleFunc("/drainers", s.AllDrainers).Methods("PUT")
	http.Handle("/", router)
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

	req.ClusterID = s.clusterID

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

	statusMap := make(map[string]*node.Status)
	for _, st := range status {
		statusMap[st.NodeID] = st
	}

	// get ts from pd
	commitTS, err := s.getTSO()
	if err != nil {
		log.Errorf("get ts from pd, error %v", err)
		return &HTTPStatus{
			ErrMsg: err.Error(),
		}
	}

	var cp binlog.Pos
	if s.cp != nil {
		cp = s.cp.pos()
	}
	return &HTTPStatus{
		StatusMap:  statusMap,
		CommitTS:   commitTS,
		CheckPoint: cp,
	}
}

// ChangeStateReq is the request struct for change state.
type ChangeStateReq struct {
	NodeID string `json:"nodeID"`
	State  string `json:"state"`
}

// ApplyAction change the pump's state, now can be pause or close.
func (s *Server) ApplyAction(w http.ResponseWriter, r *http.Request) {
	rd := render.New(render.Options{
		IndentJSON: true,
	})

	nodeID := mux.Vars(r)["nodeID"]
	action := mux.Vars(r)["action"]
	log.Infof("node %s receive action %s", nodeID, action)

	if nodeID != s.node.NodeStatus().NodeID {
		rd.JSON(w, http.StatusOK, util.ErrResponsef("invalide nodeID %s, this pump's nodeID is %s", nodeID, s.node.NodeStatus().NodeID))
		return
	}

	if s.node.NodeStatus().State != node.Online {
		rd.JSON(w, http.StatusOK, util.ErrResponsef("this pump's state is %s, apply %s failed!", s.node.NodeStatus().State, action))
		return
	}

	switch action {
	case "pause":
		s.node.NodeStatus().State = node.Pausing
	case "close":
		s.node.NodeStatus().State = node.Closing
	default:
		rd.JSON(w, http.StatusOK, util.ErrResponsef("invalide action %s", action))
		return
	}

	go s.Close()
	rd.JSON(w, http.StatusOK, util.SuccessResponse(fmt.Sprintf("apply action %s success!", action), nil))
	return
}

func (s *Server) getTSO() (int64, error) {
	ts, err := util.GetTSO(s.pdCli)
	if err != nil {
		return 0, errors.Trace(err)
	}
	// update latestTS by the way
	latestTS = ts

	return ts, nil
}

// commitStatus commit the node's last status to pd when close the server.
func (s *Server) commitStatus() {
	// update this node
	var state string
	switch s.node.NodeStatus().State {
	case node.Pausing, node.Online:
		state = node.Paused
	case node.Closing:
		state = node.Offline
	}
	status := node.NewStatus(s.node.NodeStatus().NodeID, s.node.NodeStatus().Addr, state, 0, 0)
	err := s.node.RefreshStatus(context.Background(), status)
	if err != nil {
		log.Errorf("unregister pump error %v", errors.ErrorStack(err))
	}
	log.Infof("%s has update status to %s", s.node.NodeStatus().NodeID, state)
}

// Close gracefully releases resource of pump server
func (s *Server) Close() {
	log.Info("begin to close drainer server")
	if atomic.CompareAndSwapInt32(&s.isClosed, 0, 1) == false {
		log.Debug("server had closed")
		return
	}

	// notify other goroutines to exit
	s.cancel()
	s.wg.Wait()
	log.Info("background goroutins are stopped")

	// close tiStore
	if s.pdCli != nil {
		s.pdCli.Close()
	}
	log.Info("has closed pdCli")

	if err := s.storage.Close(); err != nil {
		log.Errorf("close storage error %v", errors.ErrorStack(err))
	}

	s.commitStatus()
	if err := s.node.Quit(); err != nil {
		log.Errorf("close pump node error %s", errors.Trace(err))
	}

	// stop the gRPC server
	s.gs.GracefulStop()
	log.Info("grpc is stopped")
}
