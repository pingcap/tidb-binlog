package pump

import (
	"net"
	"net/url"

	"sync"

	"fmt"
	"os"
	"path"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
	pb "github.com/pingcap/tidb-binlog/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Pump server implements the gRPC interface,
// and maintains pump's status at run time.
type pumpServer struct {
	// RWMutex protects dispatcher
	sync.RWMutex

	// dispatcher keeps all opened binloggers which is indexed by clusterID.
	dispatcher map[string]Binlogger

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

	// node maintain the status of this pump and interact with etcd registry
	node Node
}

func newPumpServer(cfg *Config, n Node) *pumpServer {
	return &pumpServer{
		dispatcher: make(map[string]Binlogger),
		dataDir:    cfg.DataDir,
		node:       n,
	}
}

// init scan the dataDir to find all clusterIDs, and for each to create binlogger,
// then add them to dispathcer map
func (s *pumpServer) init() error {
	clusterDir := path.Join(s.dataDir, "clusters")
	if !file.Exist(clusterDir) {
		if err := os.MkdirAll(clusterDir, file.PrivateDirMode); err != nil {
			return errors.Trace(err)
		}
	}

	names, err := file.ReadDir(clusterDir)
	if err != nil {
		return errors.Trace(err)
	}

	for _, n := range names {
		binlogDir := path.Join(clusterDir, n)
		binlogger, err := OpenBinlogger(binlogDir)
		if err != nil {
			return errors.Trace(err)
		}
		s.dispatcher[n] = binlogger
	}
	return nil
}

func (s *pumpServer) getBinloggerToWrite(cid string) (Binlogger, error) {
	s.Lock()
	defer s.Unlock()
	blr, ok := s.dispatcher[cid]
	if ok {
		return blr, nil
	}
	newblr, err := CreateBinlogger(path.Join(s.dataDir, "clusters", cid))
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.dispatcher[cid] = newblr
	return newblr, nil
}

func (s *pumpServer) getBinloggerToRead(cid string) (Binlogger, error) {
	s.RLock()
	defer s.RUnlock()
	blr, ok := s.dispatcher[cid]
	if ok {
		return blr, nil
	}
	return nil, errors.NotFoundf("no binlogger of clusterID: %s", cid)
}

func (s *pumpServer) WriteBinlog(ctx context.Context, in *pb.WriteBinlogReq) (*pb.WriteBinlogResp, error) {
	cid := fmt.Sprintf("%d", in.ClusterID)
	ret := &pb.WriteBinlogResp{}
	binlogger, err := s.getBinloggerToWrite(cid)
	if err != nil {
		ret.Errmsg = err.Error()
		return ret, err
	}
	if err := binlogger.WriteTail(in.Payload); err != nil {
		ret.Errmsg = err.Error()
		return ret, err
	}
	return ret, nil
}

func (s *pumpServer) PullBinlogs(ctx context.Context, in *pb.PullBinlogReq) (*pb.PullBinlogResp, error) {
	cid := fmt.Sprintf("%d", in.ClusterID)
	ret := &pb.PullBinlogResp{}
	binlogger, err := s.getBinloggerToRead(cid)
	if err != nil {
		if errors.IsNotFound(err) {
			// return an empty slice and a nil error
			ret.Binlogs = []pb.Binlog{}
			return ret, nil
		}
		ret.Errmsg = err.Error()
		return ret, err
	}
	binlogs, err := binlogger.ReadFrom(in.StartFrom, in.Batch)
	if err != nil {
		ret.Errmsg = err.Error()
		return ret, err
	}
	ret.Binlogs = binlogs
	return ret, nil
}

// Start runs PumpServer to serve the listening port, and maintains heartbeat to Etcd
func Start(cfg *Config) {
	node, err := NewPumpNode(cfg)
	if err != nil {
		log.Fatalf("fail to create node, %v", err)
	}
	if err := node.RegisterToEtcd(context.Background()); err != nil {
		log.Fatalf("fail to register node to etcd, %v", err)
	}
	done := make(chan struct{})
	defer close(done)
	errc := node.Heartbeat(context.Background(), done)
	go func() {
		for err := range errc {
			log.Error(err)
		}
	}()

	server := newPumpServer(cfg, node)
	if err := server.init(); err != nil {
		log.Fatalf("fail to initialize pump server, %v", err)
	}

	// start to listen
	u, err := url.Parse(cfg.ListenAddr)
	if err != nil {
		log.Fatalf("invalid configuration of listening addr: %s, error: %v", cfg.ListenAddr, err)
	}
	lis, err := net.Listen("tcp", u.Host)
	if err != nil {
		log.Fatalf("fail to listen on: %s, %v", u.Host, err)
	}
	// start a gRPC server and register the pump server with it
	s := grpc.NewServer()
	pb.RegisterPumpServer(s, server)
	s.Serve(lis)
}
