package main

import (
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pump"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())

	cfg := pump.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatalf("verifying flags error, %v. See 'pump --help'.", err)
	}

	pump.InitLogger(cfg)
	pump.PrintVersionInfo()

	p, err := pump.NewServer(cfg)
	if err != nil {
		log.Fatalf("creating pump server error, %v", err)
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
		p.Close()
		os.Exit(0)
	}()

	if err := p.Start(); err != nil {
		log.Errorf("pump server error, %v", err)
		os.Exit(2)
	}
}
