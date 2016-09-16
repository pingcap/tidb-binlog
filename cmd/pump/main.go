package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pump"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())

	cfg := pump.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "verifying flags error, %v. See 'pump --help'.\n", err)
		os.Exit(2)
	}

	pump.InitLogger(cfg)
	pump.PrintVersionInfo()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("got signal [%d] to exit.", sig)
		os.Exit(0)
	}()

	pump.StartUp(cfg)
}
