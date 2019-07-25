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

	"github.com/gorilla/mux"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/node"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pump/storage"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	binlog "github.com/pingcap/tipb/go-binlog"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
	"github.com/unrolled/render"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	notifyDrainerTimeout            = time.Second * 10
	pdReconnTimes                   = 30
	earlyAlertGC                    = 20 * time.Hour
	detectDrainerCheckpointInterval = 10 * time.Minute
	// GlobalConfig is global config of pump
	GlobalConfig *globalConfig
)

// Server implements the gRPC interface,
// and maintains pump's status at run time.
type Server struct {
	dataDir string
	storage storage.Storage

	clusterID uint64

	// node maintains the status of this pump and interact with etcd registry
	node node.Node

	tcpAddr    string
	unixAddr   string
	gs         *grpc.Server
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	gcDuration time.Duration
	triggerGC  chan time.Time
	metrics    *metricClient
	// save the last time we write binlog to Storage
	// if long time not write, we can write a fake binlog
	lastWriteBinlogUnixNano int64
	pdCli                   pd.Client
	cfg                     *Config

	writeBinlogCount int64
	alivePullerCount int64

	isClosed int32
}

func init() {
	// tracing has suspicious leak problem, so disable it here.
	// it must be set before any real grpc operation.
	grpc.EnableTracing = false
	GlobalConfig = &globalConfig{
		maxMsgSize: defautMaxKafkaSize,
	}
}

// NewServer returns a instance of pump server
func NewServer(cfg *Config) (*Server, error) {
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

	grpcOpts := []grpc.ServerOption{grpc.MaxRecvMsgSize(GlobalConfig.maxMsgSize)}
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

	options := storage.DefaultOptions()
	options = options.WithKVConfig(cfg.Storage.KV)
	options = options.WithSync(cfg.Storage.GetSyncLog())
	options = options.WithStopWriteAtAvailableSpace(cfg.Storage.GetStopWriteAtAvailableSpace())

	storage, err := storage.NewAppendWithResolver(cfg.DataDir, options, tiStore, lockResolver)
	if err != nil {
		return nil, errors.Trace(err)
	}

	n, err := NewPumpNode(cfg, storage.MaxCommitTS)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Server{
		dataDir:    cfg.DataDir,
		storage:    storage,
		clusterID:  clusterID,
		node:       n,
		unixAddr:   cfg.Socket,
		tcpAddr:    cfg.ListenAddr,
		gs:         grpc.NewServer(grpcOpts...),
		ctx:        ctx,
		cancel:     cancel,
		metrics:    metrics,
		gcDuration: time.Duration(cfg.GC) * 24 * time.Hour,
		pdCli:      pdCli,
		cfg:        cfg,
		triggerGC:  make(chan time.Time),
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
	atomic.AddInt64(&s.writeBinlogCount, 1)
	return s.writeBinlog(ctx, in, false)
}

// WriteBinlog implements the gRPC interface of pump server
func (s *Server) writeBinlog(ctx context.Context, in *binlog.WriteBinlogReq, isFakeBinlog bool) (*binlog.WriteBinlogResp, error) {
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

	if !isFakeBinlog {
		state := s.node.NodeStatus().State
		if state != node.Online {
			err = errors.Errorf("no online: %v", state)
			goto errHandle
		}
	}

	err = s.storage.WriteBinlog(blog)
	if err != nil {
		goto errHandle
	}

	return ret, nil

errHandle:
	lossBinlogCacheCounter.Add(1)
	if strings.HasPrefix(err.Error(), "no online") {
		log.Warn("reject write binlog for not online state, statue: %s", s.node.NodeStatus().State)
	} else {
		log.Error("write binlog failed %+v", err)
	}
	ret.Errmsg = err.Error()
	return ret, err
}

// PullBinlogs sends binlogs in the streaming way
func (s *Server) PullBinlogs(in *binlog.PullBinlogReq, stream binlog.Pump_PullBinlogsServer) error {
	var err error
	beginTime := time.Now()
	atomic.AddInt64(&s.alivePullerCount, 1)

	log.Debug("get PullBinlogs req: ", in)
	defer func() {
		log.Debug("PullBinlogs req: ", in, " quit")
		var label string
		if err != nil {
			label = "fail"
		} else {
			label = "succ"
		}

		atomic.AddInt64(&s.alivePullerCount, -1)
		rpcHistogram.WithLabelValues("PullBinlogs", label).Observe(time.Since(beginTime).Seconds())
	}()

	if in.ClusterID != s.clusterID {
		return errors.Errorf("cluster ID are mismatch, %v vs %v", in.ClusterID, s.clusterID)
	}

	// don't use pos.Suffix now, use offset like last commitTS
	last := in.StartFrom.Offset

	gcTS := s.storage.GetGCTS()
	if last <= gcTS {
		log.Errorf("drainer request a purged binlog (gc ts = %d), request %+v, some binlog events may be loss", gcTS, in)
	}

	ctx, cancel := context.WithCancel(s.ctx)
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
			err = stream.Send(resp)
			if err != nil {
				log.Warn(err)
				return err
			}
			log.Debug("PullBinlogs send binlog payload len: ", len(data), "success")
		case <-stream.Context().Done():
			log.Debug("stream done: ", stream.Context().Err())
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
	status := node.NewStatus(s.node.NodeStatus().NodeID, s.node.NodeStatus().Addr, node.Online, 0, s.storage.MaxCommitTS(), ts)
	err = s.node.RefreshStatus(context.Background(), status)
	if err != nil {
		return errors.Annotate(err, "fail to register node to etcd")
	}

	log.Infof("register success, this pump's node id is %s", s.node.NodeStatus().NodeID)

	// notify all cisterns
	ctx, _ := context.WithTimeout(s.ctx, notifyDrainerTimeout)
	if err := s.node.Notify(ctx); err != nil {
		// if fail, refresh this node's state to paused
		status := node.NewStatus(s.node.NodeStatus().NodeID, s.node.NodeStatus().Addr, node.Paused, 0, s.storage.MaxCommitTS(), 0)
		rerr := s.node.RefreshStatus(context.Background(), status)
		if rerr != nil {
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
	tcpLis, err := net.Listen("tcp", tcpURL.Host)
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

	s.wg.Add(1)
	go s.printServerInfo()

	s.wg.Add(1)
	go s.detectDrainerCheckpoint()

	// register pump with gRPC server and start to serve listeners
	binlog.RegisterPumpServer(s.gs, s)

	if s.unixAddr != "" {
		go s.gs.Serve(unixLis)
	}

	// grpc and http will use the same tcp connection
	m := cmux.New(tcpLis)
	// sets a timeout for the read of matchers
	m.SetReadTimeout(time.Second * 10)

	grpcL := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())
	go s.gs.Serve(grpcL)

	router := mux.NewRouter()
	router.HandleFunc("/status", s.Status).Methods("GET")
	router.HandleFunc("/state/{nodeID}/{action}", s.ApplyAction).Methods("PUT")
	router.HandleFunc("/drainers", s.AllDrainers).Methods("GET")
	router.HandleFunc("/debug/binlog/{ts}", s.BinlogByTS).Methods("GET")
	router.HandleFunc("/debug/gc/trigger", s.TriggerGC).Methods("POST")
	http.Handle("/", router)
	prometheus.DefaultGatherer = registry
	http.Handle("/metrics", promhttp.Handler())

	go http.Serve(httpL, nil)

	log.Infof("start to server request on %s", s.tcpAddr)
	err = m.Serve()
	if strings.Contains(err.Error(), "use of closed network connection") {
		err = nil
	}

	return err
}

// gennerate rollback binlog can forward the drainer's latestCommitTs, and just be discarded without any side effects
func (s *Server) genFakeBinlog() (*pb.Binlog, error) {
	ts, err := s.getTSO()
	if err != nil {
		return nil, errors.Trace(err)
	}

	bl := &binlog.Binlog{
		StartTs:  ts,
		Tp:       binlog.BinlogType_Rollback,
		CommitTs: ts,
	}

	return bl, nil
}

func (s *Server) writeFakeBinlog() (*pb.Binlog, error) {
	binlog, err := s.genFakeBinlog()
	if err != nil {
		err = errors.Annotate(err, "gennerate fake binlog err")
		return nil, err
	}

	payload, err := binlog.Marshal()
	if err != nil {
		err = errors.Annotate(err, "gennerate fake binlog err")
		return nil, err
	}

	req := new(pb.WriteBinlogReq)
	req.Payload = payload

	req.ClusterID = s.clusterID

	resp, err := s.writeBinlog(s.ctx, req, true)

	if err != nil {
		err = errors.Annotate(err, "write fake binlog err")
		return nil, err
	}

	if len(resp.Errmsg) > 0 {
		err = errors.Errorf("write fake binlog err: %v", resp.Errmsg)
		return nil, err
	}

	log.Debug("write fake binlog successful")
	return binlog, nil
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
				_, err := s.writeFakeBinlog()
				if err != nil {
					log.Error("write fake binlog err: ", err)
				}
			}
			lastWriteBinlogUnixNano = atomic.LoadInt64(&s.lastWriteBinlogUnixNano)
		}
	}
}

func (s *Server) detectDrainerCheckpoint() {
	defer s.wg.Done()

	ticker := time.NewTicker(detectDrainerCheckpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			log.Info("detect drainer checkpoint routine exit")
			return
		case <-ticker.C:
			gcTS := s.storage.GetGCTS()
			alertGCMS := earlyAlertGC.Nanoseconds() / 1000 / 1000
			alertGCTS := gcTS + int64(oracle.EncodeTSO(alertGCMS))

			log.Infof("use gc ts %d to detect drainer checkpoint", gcTS)
			// detect whether the binlog before drainer's checkpoint had been purged
			s.detectDrainerCheckPoints(s.ctx, alertGCTS)
		}
	}
}

func (s *Server) printServerInfo() {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			log.Info("printServerInfo exit")
			return
		case <-ticker.C:
			log.Infof("writeBinlogCount: %d, alivePullerCount: %d,  maxCommitTS: %d",
				atomic.LoadInt64(&s.writeBinlogCount), atomic.LoadInt64(&s.alivePullerCount),
				s.storage.MaxCommitTS())
		}
	}
}

func (s *Server) gcBinlogFile() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			log.Info("gcBinlogFile exit")
			return
		case <-s.triggerGC:
			log.Info("trigger gc now")
		case <-time.After(time.Hour):
		}

		if s.gcDuration == 0 {
			continue
		}

		millisecond := time.Now().Add(-s.gcDuration).UnixNano() / 1000 / 1000
		gcTS := int64(oracle.EncodeTSO(millisecond))

		log.Infof("send gc request to storage, ts: %d", gcTS)
		s.storage.GC(gcTS)
	}
}

func (s *Server) detectDrainerCheckPoints(ctx context.Context, gcTS int64) {
	pumpNode := s.node.(*pumpNode)

	drainers, err := pumpNode.Nodes(ctx, "drainers")
	if err != nil {
		log.Error("fail to query status of drainers: %v", err)
		return
	}

	for _, drainer := range drainers {
		if drainer.State == node.Offline {
			continue
		}

		if drainer.MaxCommitTS < gcTS {
			log.Errorf("drainer(%s) checkpoint(max commit ts = %d) is older than pump gc ts(%d), some binlogs are purged", drainer.NodeID, drainer.MaxCommitTS, gcTS)
			// will add test when binlog have failpoint
			detectedDrainerBinlogPurged.WithLabelValues(drainer.NodeID).Inc()
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

	pumps, err := node.EtcdRegistry.Nodes(s.ctx, "drainers")
	if err != nil {
		log.Errorf("get pumps error %v", err)
	}

	json.NewEncoder(w).Encode(pumps)
}

// Status exposes pumps' status to HTTP handler.
func (s *Server) Status(w http.ResponseWriter, r *http.Request) {
	s.PumpStatus().Status(w, r)
}

// TriggerGC trigger pump to gc now
func (s *Server) TriggerGC(w http.ResponseWriter, r *http.Request) {
	select {
	case s.triggerGC <- time.Now():
		fmt.Fprintln(w, "trigger gc success")
	default:
		fmt.Fprintln(w, "gc is working")
	}
}

// BinlogByTS exposes api get get binlog by ts
func (s *Server) BinlogByTS(w http.ResponseWriter, r *http.Request) {
	tsStr := mux.Vars(r)["ts"]
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		w.Write([]byte(fmt.Sprintf("invalid parameter ts: %s", tsStr)))
		return
	}

	binlog, err := s.storage.GetBinlog(ts)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	w.Write([]byte(binlog.String()))
	if len(binlog.PrewriteValue) > 0 {
		prewriteValue := new(pb.PrewriteValue)
		prewriteValue.Unmarshal(binlog.PrewriteValue)

		w.Write([]byte("\n\n PrewriteValue: \n"))
		w.Write([]byte(prewriteValue.String()))
	}
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

	return &HTTPStatus{
		StatusMap: statusMap,
		CommitTS:  commitTS,
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
		log.Infof("pump %s's state change to pausing", nodeID)
		s.node.NodeStatus().State = node.Pausing
	case "close":
		log.Infof("pump %s's state change to closing", nodeID)
		s.node.NodeStatus().State = node.Closing
	default:
		rd.JSON(w, http.StatusOK, util.ErrResponsef("invalide action %s", action))
		return
	}

	go s.Close()
	rd.JSON(w, http.StatusOK, util.SuccessResponse(fmt.Sprintf("apply action %s success!", action), nil))
}

func (s *Server) getTSO() (int64, error) {
	ts, err := util.GetTSO(s.pdCli)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return ts, nil
}

// return if and only if it's safe to offline or context is canceled
func (s *Server) waitSafeToOffline(ctx context.Context) error {
	var fakeBinlog *pb.Binlog
	var err error

	// write a fake binlog
	for {
		fakeBinlog, err = s.writeFakeBinlog()
		if err != nil {
			log.Error(err)

			select {
			case <-time.After(time.Second):
				continue
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			}
		}

		break
	}

	// check storage has handle this fake binlog
	for {
		maxCommitTS := s.storage.MaxCommitTS()
		if maxCommitTS < fakeBinlog.CommitTs {
			log.Info("max commit TS in storage: %d, fake binlog commit ts: %d", maxCommitTS, fakeBinlog.CommitTs)
			select {
			case <-time.After(time.Second):
				continue
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			}
		}

		break
	}

	log.Debug("start to check offline safe for drainers")

	// check drainer has consume fake binlog we just write
	for {
		select {
		case <-time.After(time.Second):
			pumpNode := s.node.(*pumpNode)
			drainers, err := pumpNode.Nodes(ctx, "drainers")
			if err != nil {
				log.Error(err)
				return errors.Trace(err)
			}
			needByDrainer := false
			for _, drainer := range drainers {
				if drainer.State == node.Offline {
					continue
				}

				if drainer.MaxCommitTS < fakeBinlog.CommitTs {
					log.Infof("wait for drainer: %v maxCommitTS: %d, pump maxCommitTS: %d", drainer.NodeID, drainer.MaxCommitTS, fakeBinlog.CommitTs)
					needByDrainer = true
					break
				}
			}
			if !needByDrainer {
				return nil
			}

			_, err = s.writeFakeBinlog()
			if err != nil {
				log.Errorf("write fake binlog error %v", err)
			}

		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		}
	}
}

// commitStatus commit the node's last status to pd when close the server.
func (s *Server) commitStatus() {
	// update this node
	var state string
	switch s.node.NodeStatus().State {
	case node.Pausing, node.Online:
		state = node.Paused
	case node.Closing:
		s.waitSafeToOffline(context.Background())
		log.Info("safe to offline now")
		state = node.Offline
	default:
		log.Warnf("there must be something wrong, now the node status is %v", s.node.NodeStatus())
		state = s.node.NodeStatus().State
	}
	status := node.NewStatus(s.node.NodeStatus().NodeID, s.node.NodeStatus().Addr, state, 0, s.storage.MaxCommitTS(), 0)
	err := s.node.RefreshStatus(context.Background(), status)
	if err != nil {
		log.Errorf("unregister pump error %v", errors.ErrorStack(err))
	}
	log.Infof("%s has update status to %s", s.node.NodeStatus().NodeID, state)
}

// Close gracefully releases resource of pump server
func (s *Server) Close() {
	log.Info("begin to close pump server")
	if !atomic.CompareAndSwapInt32(&s.isClosed, 0, 1) {
		log.Debug("server had closed")
		return
	}

	// notify other goroutines to exit
	s.cancel()
	s.wg.Wait()
	log.Info("background goroutins are stopped")

	s.commitStatus()
	log.Info("commit status done")

	// stop the gRPC server
	s.gs.GracefulStop()
	log.Info("grpc is stopped")

	if err := s.storage.Close(); err != nil {
		log.Errorf("close storage error %v", errors.ErrorStack(err))
	}
	log.Info("storage is closed")

	if err := s.node.Quit(); err != nil {
		log.Errorf("close pump node error %s", errors.Trace(err))
	}
	log.Info("pump node is closed")

	// close tiStore
	if s.pdCli != nil {
		s.pdCli.Close()
	}
	log.Info("has closed pdCli")
}
