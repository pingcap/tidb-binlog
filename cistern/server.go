package cistern

import (
	"net"
	"net/url"
	"os"
	"path"
	"sync"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	// WindowNamespace is window namespace for store.Store
	WindowNamespace = []byte("window")
	// BinlogNamespace is binlog namespace for store.Store
	BinlogNamespace = []byte("binlog")
)

// Server implements the gRPC interface,
// and maintains the runtime status
type Server struct {
	boltdb    store.Store
	window    *DepositWindow
	collector *Collector
	publisher *Publisher
	tcpAddr   string
	gs        *grpc.Server
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewServer return a instance of binlog-server
func NewServer(cfg *Config) (*Server, error) {
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return nil, err
	}

	s, err := store.NewBoltStore(path.Join(cfg.DataDir, "data.bolt"), [][]byte{WindowNamespace, BinlogNamespace})
	if err != nil {
		return nil, errors.Annotatef(err, "failed to open boltDB store in dir(%s)", cfg.DataDir)
	}
	win, err := NewDepositWindow(s)
	if err != nil {
		return nil, errors.Trace(err)
	}
	c, err := NewCollector(cfg, s, win)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p := NewPublisher(cfg, s, win)
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		boltdb:    s,
		window:    win,
		collector: c,
		publisher: p,
		tcpAddr:   cfg.ListenAddr,
		gs:        grpc.NewServer(),
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

// DumpBinlog implements the gRPC interface of cistern server
func (s *Server) DumpBinlog(ctx context.Context, req *binlog.DumpBinlogReq) (*binlog.DumpBinlogResp, error) {
	ret := &binlog.DumpBinlogResp{}
	start := req.BeginCommitTS
	startKey := codec.EncodeInt([]byte{}, start)
	end := s.window.LoadLower()
	limit := req.Limit

	err := s.boltdb.Scan(BinlogNamespace, startKey, func(key []byte, val []byte) bool {
		if limit <= 0 {
			return false
		}

		_, cts, err := codec.DecodeInt(key)
		if err != nil {
			ret.Errmsg = err.Error()
			return false
		}

		if cts == start {
			return true
		}

		if cts >= end {
			return false
		}

		payload, _, err := decodePayload(val)
		if err != nil {
			ret.Errmsg = err.Error()
			return false
		}

		ret.Payloads = append(ret.Payloads, payload)
		ret.EndCommitTS = cts
		limit--

		return true
	})
	if err != nil {
		ret.Errmsg = err.Error()
	}

	return ret, nil
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

// Start runs CisternServer to serve the listening addr, and starts to collect binlog
func (s *Server) Start() error {
	// start to collect
	s.StartCollect()

	// start to publish
	s.StartPublish()

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
	s.gs.GracefulStop()
	// notify all goroutines to exit
	s.cancel()
	// waiting for goroutines exit
	s.wg.Wait()
	s.boltdb.Close()
}
