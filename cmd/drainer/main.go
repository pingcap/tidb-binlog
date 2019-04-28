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

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/drainer"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pkg/version"
	_ "google.golang.org/grpc/encoding/gzip"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())

	cfg := drainer.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatalf("verifying flags error, See 'drainer --help'. %s", errors.ErrorStack(err))
	}

	util.InitLogger(cfg.LogLevel, cfg.LogFile, cfg.LogRotate)
	version.PrintVersionInfo()
	log.Infof("use config: %+v", cfg)

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

	log.Info("drainer exit")
}
