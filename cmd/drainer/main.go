package main

import (
	"flag"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/drainer"
	pb "github.com/pingcap/tipb/go-binlog"
	"google.golang.org/grpc"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := drainer.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Errorf("parse cmd flags err %s\n", err)
		os.Exit(2)
	}

	if cfg.EndCommitTS > 0 && cfg.EndCommitTS <= cfg.InitCommitTS {
		log.Info("don't need recovery, the data is complete")
		os.Exit(0)
	}

	drainer.InitLogger(cfg)
	drainer.PrintVersionInfo()
	log.Infof("%v", cfg)

	cisternClient := createCisternClient(cfg.CisternAddr)

	ds, err := drainer.NewDrainer(cfg, cisternClient)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("Got signal [%d] to exit.", sig)
		ds.Close()
		os.Exit(0)
	}()

	go func() {
		err1 := http.ListenAndServe(cfg.PprofAddr, nil)
		if err1 != nil {
			log.Fatal(err1)
		}
	}()

	err = ds.Start()
	if err != nil {
		log.Error(errors.ErrorStack(err))
	}
}

func createCisternClient(cisternAddr string) pb.CisternClient {
	dialerOpt := grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("tcp", addr, timeout)
	})
	clientCon, err := grpc.Dial(cisternAddr, dialerOpt, grpc.WithInsecure())
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
	return pb.NewCisternClient(clientCon)
}
