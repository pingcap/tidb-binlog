package main

import (
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
	"github.com/pingcap/tidb-binlog/pkg/version"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())

	cfg := drainer.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatalf("verifying flags error, See 'drainer --help'. %s", errors.ErrorStack(err))
	}

	drainer.InitLogger(cfg)

	// print it whatever the config log level is
	log.SetLevelByString("debug")
	version.PrintVersionInfo()
	log.Infof("use config: %+v", cfg.String())
	log.SetLevelByString(cfg.LogLevel)

	bs, err := drainer.NewServer(cfg)
	if err != nil {
		log.Fatalf("create drainer server error, %s", errors.ErrorStack(err))
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
