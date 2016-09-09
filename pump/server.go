package pump

import (
	"golang.org/x/net/context"
	pb "github.com/iamxy/tidb-binlog/proto"
	"net"
	"google.golang.org/grpc"
	"github.com/ngaut/log"
	"fmt"
)

type server struct {}

func (s *server) WriteBinlog(ctx context.Context, in *pb.WriteBinlogReq) (*pb.WriteBinlogResp, error) {
	return nil, nil
}

func (s *server) PullBinlog(ctx context.Context, in *pb.PullBinlogReq) (*pb.PullBinlogResp, error) {
	return nil, nil
}

func Run(cfg *Config) {
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", cfg.Port))
	if err !=  nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterPumpServer(s, &server{})
	s.Serve(lis)
}
