package server

import (
	"net"
	"net/url"

	"github.com/ngaut/log"
	pb "github.com/pingcap/tidb-binlog/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type BinlogServer struct {
}

func NewBinlogServer(cfg *Config) *BinlogServer {
	return nil
}

func (bs *BinlogServer) init() error {
	return nil
}

func (bs *BinlogServer) StartCollect() {
}

func (bs *BinlogServer) StartPublish() {
}

func (bs *BinlogServer) StartGC() {
}

func (bs *BinlogServer) DumpBinlog(context.Context, *pb.DumpBinlogReq) (*pb.DumpBinlogResp, error) {
	return nil, nil
}

func Start(cfg *Config) {
	server := NewBinlogServer(cfg)
	if err := server.init(); err != nil {
		log.Fatalf("fail to initialize binlog server, %v", err)
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
	pb.RegisterBinlogServer(s, server)
	s.Serve(lis)
}
