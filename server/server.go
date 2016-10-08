package server

import (
	"net"
	"net/url"
	"sync"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/pkg/store"
	pb "github.com/pingcap/tidb-binlog/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// BinlogServer implements the gRPC interface,
// and maintains the runtime status
type BinlogServer struct {
	rocksdb   store.Store
	window    *DepositWindow
	collector *Collector
	publisher *Publisher
	tcpAddr   string
	gs        *grpc.Server
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewBinlogServer return a instance of binlog-server
func NewBinlogServer(cfg *Config) (*BinlogServer, error) {
	s, err := store.NewRocksStore(cfg.DataDir)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to open RocksDB store in dir(%s)", cfg.DataDir)
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
	return &BinlogServer{
		rocksdb:   s,
		window:    win,
		collector: c,
		publisher: p,
		tcpAddr:   cfg.ListenAddr,
		gs:        grpc.NewServer(),
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

// DumpBinlog implements the gRPC interface of binlog-server
func (s *BinlogServer) DumpBinlog(ctx context.Context, req *pb.DumpBinlogReq) (*pb.DumpBinlogResp, error) {
	ret := &pb.DumpBinlogResp{}
	start := req.BeginCommitTS
	end := s.window.LoadLower()
	limit := req.Limit

	iter, err := s.rocksdb.Scan(start)
	if err != nil {
		ret.Errmsg = err.Error()
		return ret, nil
	}
	defer iter.Close()
	for ; iter.Valid() && limit > 0; iter.Next() {
		cts, err := iter.CommitTs()
		if err != nil {
			ret.Errmsg = err.Error()
			return ret, nil
		}
		// skip the one of start position
		if cts == start {
			continue
		}
		if cts >= end {
			break
		}
		payload, _, err := iter.Payload()
		if err != nil {
			ret.Errmsg = err.Error()
			return ret, nil
		}
		ret.Payloads = append(ret.Payloads, payload)
		ret.EndCommitTS = cts
		limit--
	}
	return ret, nil
}

// StartCollect runs Collector up in a coroutine.
func (s *BinlogServer) StartCollect() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.collector.Start(s.ctx)
	}()
}

// StartPublish runs Publisher up in a coroutine.
func (s *BinlogServer) StartPublish() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.publisher.Start(s.ctx)
	}()
}

// Start runs BinlogServer to serve the listening addr, and starts to collect binlog
func (s *BinlogServer) Start() error {
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

	// register binlog-server with gRPC server and start to serve listener
	pb.RegisterBinlogServer(s.gs, s)
	s.gs.Serve(tcpLis)
	return nil
}

// Close stops all coroutines started by binlog-server gracefully
func (s *BinlogServer) Close() {
	// first stop gRPC server
	s.gs.GracefulStop()
	// notify all coroutines to exit
	s.cancel()
	// waiting for coroutines exit
	s.wg.Wait()
	s.rocksdb.Close()
}
