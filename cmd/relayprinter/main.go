// Copyright 2020 PingCAP, Inc.
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
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/version"
	"github.com/pingcap/tidb-binlog/relayprinter"
	"go.uber.org/zap"
)

func main() {
	cfg := relayprinter.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatal("verify flags failed, see 'relayprinter --help'.", zap.Error(err))
	}

	version.PrintVersionInfo("relay-printer")

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	p := relayprinter.NewPrinter(cfg)

	go func() {
		sig := <-sc
		log.Info("got signal to exit.", zap.Stringer("signal", sig))
		p.Close()
		os.Exit(0)
	}()

	if err := p.Process(); err != nil {
		log.Error("relay-printer process failed", zap.Error(err))
	}
	p.Close()
}
