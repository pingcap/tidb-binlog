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
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/version"
	reparo "github.com/pingcap/tidb-binlog/reparo"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())

	cfg := reparo.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatalf("verifying flags error, %v. See 'reparo --help'.", err)
	}

	reparo.InitLogger(cfg)
	version.PrintVersionInfo()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	r, err := reparo.New(cfg)
	if err != nil {
		log.Fatalf("create reparo err %v", errors.ErrorStack(err))
	}

	go func() {
		sig := <-sc
		log.Infof("got signal [%v] to exit.", sig)
		r.Close()
		os.Exit(0)
	}()

	if err := r.Process(); err != nil {
		log.Errorf("reparo processing error, %v", errors.ErrorStack(err))
	}
	if err := r.Close(); err != nil {
		log.Fatalf("close err %v", err)
	}
}
