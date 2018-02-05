package main

import (
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/version"
	"github.com/pingcap/tidb-binlog/restore"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())

	cfg := restore.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatalf("verifying flags error, %v. See 'restore --help'.", err)
	}

	restore.InitLogger(cfg)
	version.PrintVersionInfo()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	r, err := restore.New(cfg)
	if err != nil {
		log.Fatalf("create restore err %v", err)
	}

	go func() {
		sig := <-sc
		log.Infof("got signal [%v] to exit.", sig)
		r.Close()
		os.Exit(0)
	}()

	if err := r.Start(); err != nil {
		log.Errorf("restore start error, %v", errors.ErrorStack(err))
		os.Exit(2)
	}
}
