package pump

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/pd/pd-client"
	bf "github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tipb/go-binlog"
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
	// dispatcher keeps all opened binloggers which is indexed by clusterID.
	dispatcher Binlogger

	// dataDir is the root directory of all pump data
	// |
	// +-- .node
	// |   |
	// |   +-- nodeID
	// |
	// +-- clusters
	//     |
	//     +-- 100
	//     |   |
	//     |   +-- binlog.000001
	//     |   |
	//     |   +-- binlog.000002
	//     |   |
	//     |   +-- ...
	//     |
	//     +-- 200
	//         |
	//         +-- binlog.000001
	//         |
	//         +-- binlog.000002
	//         |
	//         +-- ...
	//
	dataDir string

	clusterID string

	// node maintains the status of this pump and interact with etcd registry
	node Node

	tcpAddr  string
	unixAddr string
	gs       *grpc.Server
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	gc       time.Duration
	metrics  *metricClient
	// it would be set false while there are new binlog coming, would be set true every genBinlogInterval
	needGenBinlog AtomicBool
	pdCli         pd.Client
	cfg           *Config

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

	return &Server{
		dataDir:   cfg.DataDir,
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

// inits scans the dataDir to find all clusterIDs, and creates binlogger for each,
// then adds them to dispathcer map
func (s *Server) init() error {
	// init cluster data dir if not exist
	var err error
	clusterDir := path.Join(s.dataDir, "clusters")
	if !bf.Exist(clusterDir) {
		if err := os.MkdirAll(clusterDir, file.PrivateDirMode); err != nil {
			return errors.Trace(err)
		}
	}

	s.dispatcher, err = s.getBinloggerToWrite()
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (s *Server) getBinloggerToWrite() (Binlogger, error) {
	if s.dispatcher != nil {
		return s.dispatcher, nil
	}

	// use tiStore's currentVersion method to get the ts from tso
	addrs, err := flags.ParseHostPortAddr(s.cfg.KafkaAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	kb, err := createKafkaBinlogger(s.clusterID, s.node.ID(), addrs, s.cfg.KafkaVersion)
	if err != nil {
		return nil, errors.Trace(err)
	}

	switch s.cfg.WriteMode {
	case kafkaWriteMode:
		log.Debug("send binlog to kafka directly")
		s.dispatcher = kb
		return kb, nil
	case mixedWriteMode:
		binlogDir := path.Join(path.Join(s.dataDir, "clusters"), s.clusterID)

		fb, err := OpenBinlogger(binlogDir, compress.CompressionNone) // no compression now.
		if err != nil {
			return nil, errors.Trace(err)
		}

		cp, err := newCheckPoint(path.Join(binlogDir, "checkpoint"))
		if err != nil {
			return nil, errors.Trace(err)
		}

		s.cp = cp
		s.dispatcher = newProxy(s.node.ID(), fb, kb, cp)
		return s.dispatcher, nil

	default:
		return nil, errors.New("unsupport write mode")
	}
}

func (s *Server) getBinloggerToRead() (Binlogger, error) {
	if s.dispatcher != nil {
		return s.dispatcher, nil
	}
	return nil, errors.NotFoundf("no binlogger of clusterID: %s", s.clusterID)
}

// WriteBinlog implements the gRPC interface of pump server
func (s *Server) WriteBinlog(ctx context.Context, in *binlog.WriteBinlogReq) (*binlog.WriteBinlogResp, error) {
	var err error
	beginTime := time.Now()
	defer func() {
		var label string
		if err != nil {
			label = "fail"
		} else {
			label = "succ"
		}

		rpcHistogram.WithLabelValues("WriteBinlog", label).Observe(time.Since(beginTime).Seconds())
	}()

	s.needGenBinlog.Set(false)

	cid := fmt.Sprintf("%d", in.ClusterID)
	if cid != s.clusterID {
		return nil, errors.Errorf("cluster ID are mismatch, %v vs %v", cid, s.clusterID)
	}

	ret := &binlog.WriteBinlogResp{}
	binlogger, err1 := s.getBinloggerToWrite()
	if err1 != nil {
		ret.Errmsg = err1.Error()
		err = errors.Trace(err1)
		return ret, err
	}

	if _, err1 := binlogger.WriteTail(&binlog.Entity{Payload: in.Payload}); err1 != nil {
		lossBinlogCacheCounter.Add(1)
		log.Errorf("write binlog error %v in %s mode", err1, s.cfg.WriteMode)

		if !s.cfg.EnableTolerant {
			ret.Errmsg = err1.Error()
			err = errors.Trace(err1)
			return ret, err
		}
	}

	return ret, nil
}

// PullBinlogs sends binlogs in the streaming way
// the rpc looks no use now, drainer read from kafka directly
func (s *Server) PullBinlogs(in *binlog.PullBinlogReq, stream binlog.Pump_PullBinlogsServer) error {
	cid := fmt.Sprintf("%d", in.ClusterID)
	if cid != s.clusterID {
		return errors.Errorf("cluster ID are mismatch, %v vs %v", cid, s.clusterID)
	}

	binlogger, err := s.getBinloggerToRead()
	if err != nil {
		return errors.Trace(err)
	}

	pos := in.StartFrom
	sendBinlog := func(entity *binlog.Entity) error {
		pos.Suffix = entity.Pos.Suffix
		pos.Offset = entity.Pos.Offset
		resp := &binlog.PullBinlogResp{Entity: *entity}
		return errors.Trace(stream.Send(resp))
	}

	for {
		err = binlogger.Walk(s.ctx, pos, sendBinlog)
		if err != nil {
			return errors.Trace(err)
		}

		select {
		// walk return nil even if ctx is Done, so check and return here
		case <-s.ctx.Done():
			return nil
		// sleep 50 ms to prevent cpu occupied
		case <-time.After(pullBinlogInterval):
		}
	}
}

// Start runs Pump Server to serve the listening addr, and maintains heartbeat to Etcd
func (s *Server) Start() error {
	// register this node
	if err := s.node.Register(s.ctx); err != nil {
		return errors.Annotate(err, "fail to register node to etcd")
	}

	// notify all cisterns
	if err := s.node.Notify(s.ctx); err != nil {
		// if fail, unregister this node
		if err := s.node.Unregister(context.Background()); err != nil {
			log.Errorf("unregister pump while pump fails to notify drainer error %v", errors.ErrorStack(err))
		}
		return errors.Annotate(err, "fail to notify all living drainer")
	}

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
	// there are only one binlogger for the specified cluster
	// so we can use only one needGenBinlog flag
	if s.needGenBinlog.Get() {
		binlogger, err := s.getBinloggerToWrite()
		if err != nil {
			log.Errorf("generate forward binlog, get binlogger err %v", err)
			return
		}
		payload, err := s.genFakeBinlog()
		if err != nil {
			log.Errorf("generate forward binlog, generate binlog err %v", err)
			return
		}

		_, err = binlogger.WriteTail(&binlog.Entity{Payload: payload})
		if err != nil {
			log.Errorf("generate forward binlog, write binlog err %v", err)
			return
		}

		log.Infof("generate fake binlog successfully")

	}

	s.needGenBinlog.Set(true)
}

// we would generate binlog to forward the pump's latestCommitTs in drainer when there is no binlogs in this pump
func (s *Server) genForwardBinlog() {
	defer s.wg.Done()

	s.needGenBinlog.Set(true)
	genFakeBinlogInterval := time.Duration(s.cfg.GenFakeBinlogInterval) * time.Second
	for {
		select {
		case <-s.ctx.Done():
			log.Info("genFakeBinlog exit")
			return
		case <-time.After(genFakeBinlogInterval):
			s.writeFakeBinlog()
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
			if s.dispatcher != nil {
				s.dispatcher.GC(s.gc, binlog.Pos{})
			}
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

	// will write binlog to dispatcher if genFakeBinlog() is still running or the pump server is serving
	// so close this at lastest
	if s.dispatcher != nil {
		if err := s.dispatcher.Close(); err != nil {
			log.Errorf("close binlogger error %v", err)
		}
	}
}
