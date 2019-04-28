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
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pkg/version"
	"go.uber.org/zap"
	_ "google.golang.org/grpc/encoding/gzip"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())

	cfg := drainer.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatal("verifying flags failed, See 'drainer --help'.", zap.Error(err))
	}

	util.InitLogger(cfg.LogLevel, cfg.LogFile)
	version.PrintVersionInfo()
	log.Info("start drainer...", zap.Reflect("config", cfg))

	bs, err := drainer.NewServer(cfg)
	if err != nil {
		log.Fatal("create drainer server failed", zap.Error(err))
	}

	sc := make(chan os.Signal, 1)

	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Info("got signal to exit.", zap.Stringer("signal", sig))
		bs.Close()
		os.Exit(0)
	}()

	if err := bs.Start(); err != nil {
		log.Error("start drainer server failed", zap.Error(err))
		os.Exit(2)
	}

	log.Info("drainer exit")
}
