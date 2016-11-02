package cistern

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/soheilhy/cmux"
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
func (s *Server) DumpBinlog(req *binlog.DumpBinlogReq, stream binlog.Cistern_DumpBinlogServer) (err error) {
	beginTime := time.Now()
	defer func() {
		var label string
		if err != nil {
			label = "fail"
		} else {
			label = "succ"
		}
		rpcHistogram.WithLabelValues("DumpBinlog", label).Observe(time.Since(beginTime).Seconds())
		rpcCounter.WithLabelValues("DumpBinlog", label).Add(1)
	}()

	batch := 1000
	latest := req.BeginCommitTS

	for {
		end := s.window.LoadLower()
		if latest >= end {
			time.Sleep(1 * time.Second)
			continue
		}

		var resps []*binlog.DumpBinlogResp
		err = s.boltdb.Scan(
			binlogNamespace,
			codec.EncodeInt([]byte{}, latest),
			func(key []byte, val []byte) (bool, error) {
				_, cts, err1 := codec.DecodeInt(key)
				if err1 != nil {
					return false, errors.Trace(err1)
				}
				if cts > end || len(resps) >= batch {
					return false, nil
				}
				if cts == latest {
					return true, nil
				}
				payload, _, err1 := decodePayload(val)
				if err1 != nil {
					return false, errors.Trace(err1)
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
			log.Errorf("gRPC: DumpBinlog scan boltdb error, %s", errors.ErrorStack(err))
			err = errors.Trace(err)
			return
		}

		for _, resp := range resps {
			item := &binlog.Binlog{}
			if err = item.Unmarshal(resp.Payload); err != nil {
				log.Errorf("gRPC: DumpBinlog unmarshal binlog error, %s", errors.ErrorStack(err))
				err = errors.Trace(err)
				return
			}
			if item.DdlJobId > 0 {
				key := codec.EncodeInt([]byte{}, item.DdlJobId)
				data, err1 := s.boltdb.Get(ddlJobNamespace, key)
				if err1 != nil {
					log.Errorf("DDL Job(%d) not found, with binlog commitTS(%d), %s", item.DdlJobId, resp.CommitTS, errors.ErrorStack(err1))
					return errors.Annotatef(err,
						"DDL Job(%d) not found, with binlog commitTS(%d)", item.DdlJobId, resp.CommitTS)
				}
				resp.Ddljob = data
			}
			if err = stream.Send(resp); err != nil {
				log.Errorf("gRPC: DumpBinlog send stream error, %s", errors.ErrorStack(err))
				err = errors.Trace(err)
				return
			}
			latest = resp.CommitTS
		}
	}
}

// GetLatestCommitTS implements the gRPC interface of cistern server
func (s *Server) GetLatestCommitTS(ctx context.Context, req *binlog.GetLatestCommitTSReq) (*binlog.GetLatestCommitTSResp, error) {
	status := s.collector.HTTPStatus()
	return &binlog.GetLatestCommitTSResp{
		IsSynced: status.Synced,
		CommitTS: status.DepositWindow.Upper,
	}, nil
}

// DumpDDLJobs implements the gRPC interface of cistern server
func (s *Server) DumpDDLJobs(ctx context.Context, req *binlog.DumpDDLJobsReq) (resp *binlog.DumpDDLJobsResp, err error) {
	beginTime := time.Now()
	defer func() {
		var label string
		if err != nil {
			label = "fail"
		} else {
			label = "succ"
		}
		rpcHistogram.WithLabelValues("DumpDDLJobs", label).Observe(time.Since(beginTime).Seconds())
		rpcCounter.WithLabelValues("DumpDDLJobs", label).Add(1)
	}()
	upperTS := req.BeginCommitTS
	lowerTS := calculatePreviousHourTimestamp(upperTS)

	var (
		lastTS       int64
		lastDDLJobID int64
	)

	err = s.boltdb.Scan(
		binlogNamespace,
		codec.EncodeInt([]byte{}, lowerTS),
		func(key []byte, val []byte) (bool, error) {
			_, cts, err1 := codec.DecodeInt(key)
			if err1 != nil {
				return false, errors.Trace(err)
			}
			if cts > upperTS && lastTS > 0 {
				return false, nil
			}
			payload, _, err1 := decodePayload(val)
			if err1 != nil {
				return false, errors.Trace(err1)
			}
			item := &binlog.Binlog{}
			if err1 := item.Unmarshal(payload); err1 != nil {
				return false, errors.Trace(err1)
			}
			if item.DdlJobId > 0 {
				lastDDLJobID = item.DdlJobId
			}
			lastTS = cts
			return true, nil
		},
	)
	if err != nil {
		log.Errorf("gRPC: DumpDDLJobs scan boltdb error, %v", errors.ErrorStack(err))
		err = errors.Trace(err)
		return
	}

	if lastDDLJobID > 0 {
		// If the exceed flag is true means that can't find a binlog which commitTS less or equal than the given position.
		// In this situation it should grab the first one whose commitTS is greater than the begin TS, and have to
		// do some special treatment in getAllHistoryDDLJobsByID()
		exceed := lastTS > req.BeginCommitTS
		return s.getAllHistoryDDLJobsByID(lastDDLJobID, exceed)
	}

	if lastTS > 0 {
		return s.getAllHistoryDDLJobsByTS(lastTS)
	}

	err = errors.Errorf("can't determine the schema version by incoming TS, because there is not any binlog yet.")
	return
}

func (s *Server) getAllHistoryDDLJobsByID(upperJobID int64, exceed bool) (*binlog.DumpDDLJobsResp, error) {
	ddlJobs := [][]byte{}
	err := s.boltdb.Scan(
		ddlJobNamespace,
		codec.EncodeInt([]byte{}, 0),
		func(key []byte, val []byte) (bool, error) {
			_, id, err := codec.DecodeInt(key)
			if err != nil {
				return false, errors.Trace(err)
			}
			// if exceed is true the one with the upperJobID must be excluded
			if exceed && id >= upperJobID {
				return false, nil
			} else if id > upperJobID {
				return false, nil
			}
			ddlJobs = append(ddlJobs, val)
			return true, nil
		},
	)
	if err != nil {
		log.Errorf("gRPC: DumpDDLJobs getAllHistoryDDLJobsByID error, %v", errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}

	resp := &binlog.DumpDDLJobsResp{
		Ddljobs: ddlJobs,
	}
	return resp, nil
}

func (s *Server) getAllHistoryDDLJobsByTS(ts int64) (*binlog.DumpDDLJobsResp, error) {
	val, err := s.boltdb.Get(binlogNamespace, codec.EncodeInt([]byte{}, ts))
	if err != nil {
		log.Errorf("gRPC: DumpDDLJobs getAllHistoryDDLJobsByTS get boltdb error, %v", errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}
	payload, _, err1 := decodePayload(val)
	if err1 != nil {
		log.Errorf("gRPC: DumpDDLJobs getAllHistoryDDLJobsByTS decode payload error, %v", errors.ErrorStack(err1))
		return nil, errors.Trace(err1)
	}
	item := &binlog.Binlog{}
	err = item.Unmarshal(payload)
	if err != nil {
		log.Errorf("gRPC: DumpDDLJobs getAllHistoryDDLJobsByTS unmarshal payload error, %v", errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}
	if item.Tp != binlog.BinlogType_Commit {
		log.Errorf("gRPC: DumpDDLJobs getAllHistoryDDLJobsByTS error, can't find a valid DML binlog by commitTS(%d)", ts)
		return nil, errors.Errorf("can't find a valid DML binlog by commitTS(%d)", ts)
	}
	prewriteValue := &binlog.PrewriteValue{}
	err = prewriteValue.Unmarshal(item.PrewriteValue)
	if err != nil {
		log.Errorf("gRPC: DumpDDLJobs getAllHistoryDDLJobsByTS unmarshal prewriteValue error, %v", errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}
	upperSchemaVer := prewriteValue.SchemaVersion

	ddlJobs := [][]byte{}
	err = s.boltdb.Scan(
		ddlJobNamespace,
		codec.EncodeInt([]byte{}, 0),
		func(key []byte, val []byte) (bool, error) {
			job := &model.Job{}
			if err1 := job.Decode(val); err1 != nil {
				return false, errors.Trace(err1)
			}
			var ver int64
			if err1 := job.DecodeArgs(&ver); err1 != nil {
				return false, errors.Trace(err1)
			}
			if ver > upperSchemaVer {
				return false, nil
			}
			ddlJobs = append(ddlJobs, val)
			return true, nil
		},
	)
	if err != nil {
		log.Errorf("gRPC: DumpDDLJobs getAllHistoryDDLJobsByTS scan boltdb error, %v", errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}

	resp := &binlog.DumpDDLJobsResp{
		Ddljobs: ddlJobs,
	}
	return resp, nil
}

func calculatePreviousHourTimestamp(current int64) int64 {
	physical := oracle.ExtractPhysical(uint64(current))
	prevPhysical := physical - int64(1*time.Hour/time.Millisecond)
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
		ticker := time.NewTicker(time.Hour)
		defer s.wg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				err := GCHistoryBinlog(s.boltdb, binlogNamespace, s.gc)
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
	m := cmux.New(tcpLis)
	grpcL := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())

	// register cistern server with gRPC server and start to serve listener
	binlog.RegisterCisternServer(s.gs, s)
	go s.gs.Serve(grpcL)

	http.HandleFunc("/status", s.collector.Status)
	go http.Serve(httpL, nil)

	return m.Serve()
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
