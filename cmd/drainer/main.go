package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb-binlog/drainer"
	"github.com/pingcap/tidb/store/tikv"
	pb "github.com/pingcap/tipb/go-binlog"
	"google.golang.org/grpc"
)

func main() {
	tidb.RegisterStore("tikv", tikv.Driver{})
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

	log.SetLevelByString(cfg.LogLevel)

	if len(cfg.LogFile) > 0 {
		log.SetOutputByName(cfg.LogFile)
		log.SetHighlighting(false)

		if cfg.LogRotate == "hour" {
			log.SetRotateByHour()
		} else {
			log.SetRotateByDay()
		}
	}

	log.Infof("%v", cfg)
	storePath := fmt.Sprintf("tikv://%s", cfg.PdPath)
	store, err := tidb.NewStore(storePath)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	cisternClient := createCisternClient(cfg.CisternClient.Host, cfg.CisternClient.Port)

	ds, err := drainer.NewDrainer(cfg, store, cisternClient)
	if err != nil {
		store.Close()
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

func createCisternClient(host string, port int) pb.CisternClient {
	path := fmt.Sprintf("%s:%d", host, port)
	dialerOpt := grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("tcp", addr, timeout)
	})
	clientCon, err := grpc.Dial(path, dialerOpt, grpc.WithInsecure())
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
	return pb.NewCisternClient(clientCon)
}
