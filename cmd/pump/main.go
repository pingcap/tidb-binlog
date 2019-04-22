// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pkg/version"
	"github.com/pingcap/tidb-binlog/pump"
	"go.uber.org/zap"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())

	cfg := pump.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatal("verifying flags failed. See 'pump --help'.", zap.Error(err))
	}

	util.InitLogger(cfg.LogLevel, cfg.LogFile)
	version.PrintVersionInfo()

	p, err := pump.NewServer(cfg)
	if err != nil {
		log.Fatal("creating pump server failed", zap.Error(err))
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var wg sync.WaitGroup

	go func() {
		sig := <-sc
		log.Info("got signal to exit.", zap.Stringer("signal", sig))
		wg.Add(1)
		p.Close()
		log.Info("pump is closed")
		wg.Done()
	}()

	// Start will block until the server is closed
	if err := p.Start(); err != nil {
		log.Error("start pump server failed", zap.Error(err))
		// exit when start fail
		os.Exit(2)
	}

	wg.Wait()
	log.Info("pump exit")
}
