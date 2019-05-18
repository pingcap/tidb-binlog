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
	"flag"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	"github.com/pingcap/tidb-binlog/tests/dailytest"
	"github.com/pingcap/tidb-binlog/tests/util"
	_ "github.com/zanmato1984/clickhouse"
)

func main() {
	cfg := util.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.S().Errorf("parse cmd flags err %s\n", err)
		os.Exit(2)
	}

	sourceDB, err := util.CreateDB(cfg.SourceDBCfg)
	if err != nil {
		log.S().Fatal(err)
	}
	defer func() {
		if err := util.CloseDB(sourceDB); err != nil {
			log.S().Errorf("Failed to close source database: %s\n", err)
		}
	}()

	targetAddr, err := pkgsql.ParseCHAddr(cfg.TargetDBCfg.Host)
	if err != nil {
		log.S().Fatal(err)
	}
	if len(targetAddr) != 1 {
		log.S().Fatal("only support 1 flash node so far.")
	}
	targetDB, err := pkgsql.OpenCH(targetAddr[0].Host, targetAddr[0].Port, cfg.TargetDBCfg.User, cfg.TargetDBCfg.Password, "default", 0)
	if err != nil {
		log.S().Fatal(err)
	}
	defer func() {
		if err := util.CloseDB(targetDB); err != nil {
			log.S().Errorf("Failed to close target database: %s\n", err)
		}
	}()
	_, err = targetDB.Exec(fmt.Sprintf("drop database if exists %s", cfg.TargetDBCfg.Name))
	if err != nil {
		log.S().Fatal(err)
	}
	_, err = targetDB.Exec(fmt.Sprintf("create database %s", cfg.TargetDBCfg.Name))
	if err != nil {
		log.S().Fatal(err)
	}
	_, err = targetDB.Exec(fmt.Sprintf("use %s", cfg.TargetDBCfg.Name))
	if err != nil {
		log.S().Fatal(err)
	}

	dailytest.Run(sourceDB, targetDB, cfg.TargetDBCfg.Name, cfg.WorkerCount, cfg.JobCount, cfg.Batch)
}
