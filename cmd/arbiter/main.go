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
	"io/ioutil"
	stdlog "log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/arbiter"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb-binlog/pkg/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func main() {
	cfg := arbiter.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatal("verifying flags failed. See 'arbiter --help'.", zap.Error(err))
	}

	if err := util.InitLogger(cfg.LogLevel, cfg.LogFile); err != nil {
		log.Fatal("Failed to initialize log", zap.Error(err))
	}

	// We have set sarama.Logger in util.InitLogger.
	if !cfg.OpenSaramaLog {
		// may too many noise, discard sarama log now
		sarama.Logger = stdlog.New(ioutil.Discard, "[Sarama] ", stdlog.LstdFlags)
	}

	log.Info("start arbiter...", zap.Reflect("config", cfg))
	version.PrintVersionInfo("Arbiter")

	go startHTTPServer(cfg.ListenAddr)

	srv, err := arbiter.NewServer(cfg)
	if err != nil {
		log.Error("new server failed", zap.Error(err))
		return
	}

	util.SetupSignalHandler(func(_ os.Signal) {
		srv.Close()
	})

	log.Info("start run server...")
	err = srv.Run()
	if err != nil {
		log.Error("run server failed", zap.Error(err))
	}

	log.Info("server exit")
}

func startHTTPServer(addr string) {
	prometheus.DefaultGatherer = arbiter.Registry
	http.Handle("/metrics", promhttp.Handler())

	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal("listen and server http failed", zap.String("addr", addr), zap.Error(err))
	}
}
