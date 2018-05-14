package main

import (
	"os"
	"os/signal"
	"runtime"
	"syscall"

	_ "net/http/pprof"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/version"
	reparo "github.com/pingcap/tidb-binlog/reparo"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := reparo.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatalf("verifying flags error, %v. See 'reparo --help'.", err)
	}

	reparo.InitLogger(cfg)
	version.PrintVersionInfo()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	r, err := reparo.New(cfg)
	if err != nil {
		log.Fatalf("create reparo err %v", errors.ErrorStack(err))
	}

	go func() {
		sig := <-sc
		log.Infof("got signal [%v] to exit.", sig)
		r.Close()
		os.Exit(0)
	}()

	if err := r.Process(); err != nil {
		log.Errorf("reparo processing error, %v", errors.ErrorStack(err))
	}
	if err := r.Close(); err != nil {
		log.Fatalf("close err %v", err)
	}
}
