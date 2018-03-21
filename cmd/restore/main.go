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
		log.Fatalf("create restore err %v", errors.ErrorStack(err))
	}

	go func() {
		sig := <-sc
		log.Infof("got signal [%v] to exit.", sig)
		r.Close()
		os.Exit(0)
	}()

	if err := r.Process(); err != nil {
		log.Errorf("restore processing error, %v", errors.ErrorStack(err))
	}
	if err := r.Close(); err != nil {
		log.Fatalf("close err %v", err)
	}
}
