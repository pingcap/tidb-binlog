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
	"github.com/pingcap/tidb-binlog/cistern"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())

	cfg := cistern.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "verifying flags error, See 'cistern --help'.\n %s", errors.ErrorStack(err))
		os.Exit(2)
	}

	cistern.InitLogger(cfg.Debug)
	if len(cfg.LogFile) > 0 {
		log.SetOutputByName(cfg.LogFile)

		if cfg.LogRotate == "hour" {
			log.SetRotateByHour()
		} else {
			log.SetRotateByDay()
		}
	}

	cistern.PrintVersionInfo()

	bs, err := cistern.NewServer(cfg)
	if err != nil {
		log.Errorf("create cistern server error, %s", errors.ErrorStack(err))
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
		log.Errorf("start cistern server error, %s", errors.ErrorStack(err))
		os.Exit(2)
	}
}
