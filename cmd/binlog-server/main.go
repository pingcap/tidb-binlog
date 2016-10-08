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
	"github.com/pingcap/tidb-binlog/server"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())

	cfg := server.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "verifying flags error, %v. See 'binlog-server --help'.\n", err)
		os.Exit(2)
	}

	server.InitLogger(cfg.Debug)
	server.PrintVersionInfo()

	bs, err := server.NewBinlogServer(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create binlog server error, %v", err)
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
		fmt.Fprintf(os.Stderr, "start binlog-server error, %v", err)
		os.Exit(2)
	}
}
