package main

import (
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/drainer"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())

	cfg := drainer.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "verifying flags error, See 'drainer --help'.\n %s", errors.ErrorStack(err))
		os.Exit(2)
	}

	drainer.InitLogger(cfg)
	drainer.PrintVersionInfo()

	bs, err := drainer.NewServer(cfg)
	if err != nil {
		log.Errorf("create drainer server error, %s", errors.ErrorStack(err))
		os.Exit(2)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("got signal [%d] to exit.", sig)
		bs.Close()
		os.Exit(0)
	}()

	if err := bs.Start(); err != nil {
		log.Errorf("start drainer server error, %s", errors.ErrorStack(err))
		os.Exit(2)
	}
}
