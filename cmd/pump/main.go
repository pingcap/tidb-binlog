package main

import (
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/version"
	"github.com/pingcap/tidb-binlog/pump"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())

	cfg := pump.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatalf("verifying flags error, %v. See 'pump --help'.", errors.ErrorStack(err))
	}

	pump.InitLogger(cfg)
	version.PrintVersionInfo()

	p, err := pump.NewServer(cfg)
	if err != nil {
		log.Fatalf("creating pump server error, %v", errors.ErrorStack(err))
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGKILL,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var wg sync.WaitGroup

	go func() {
		sig := <-sc
		log.Infof("got signal [%d] to exit.", sig)
		wg.Add(1)
		p.Close()
		log.Info("pump is closed")
		wg.Done()
	}()

	// Start will block until the server is closed
	if err := p.Start(); err != nil {
		log.Errorf("pump server error, %v", err)
		// exit when start fail
		return
	}

	wg.Wait()
	log.Info("pump exit")
}
