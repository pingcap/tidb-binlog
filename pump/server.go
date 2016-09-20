package pump

import (
	"net"
	"net/url"

	"github.com/ngaut/log"
	pb "github.com/pingcap/tidb-binlog/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type server struct{}

func (s *server) WriteBinlog(ctx context.Context, in *pb.WriteBinlogReq) (*pb.WriteBinlogResp, error) {
	return nil, nil
}

func (s *server) PullBinlogs(ctx context.Context, in *pb.PullBinlogReq) (*pb.PullBinlogResp, error) {
	return nil, nil
}

func Start(cfg *Config) {
	u, err := url.Parse(cfg.ListenAddr)
	if err != nil {
		log.Fatalf("bad configuration of listening addr: %s, error: %v", cfg.ListenAddr, err)
	}
	lis, err := net.Listen("tcp", u.Host)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterPumpServer(s, &server{})
	s.Serve(lis)
}
