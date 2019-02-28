package main

import (
	"flag"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
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
		log.Errorf("parse cmd flags err %s\n", err)
		os.Exit(2)
	}

	sourceDB, err := util.CreateDB(cfg.SourceDBCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer util.CloseDB(sourceDB)

	targetAddr, err := pkgsql.ParseCHAddr(cfg.TargetDBCfg.Host)
	if err != nil {
		log.Fatal(err)
	}
	if len(targetAddr) != 1 {
		log.Fatal("only support 1 flash node so far.")
	}
	targetDB, err := pkgsql.OpenCH(targetAddr[0].Host, targetAddr[0].Port, cfg.TargetDBCfg.User, cfg.TargetDBCfg.Password, "default", 0)
	if err != nil {
		log.Fatal(err)
	}
	defer util.CloseDB(targetDB)
	_, err = targetDB.Exec(fmt.Sprintf("drop database if exists %s", cfg.TargetDBCfg.Name))
	if err != nil {
		log.Fatal(err)
	}
	_, err = targetDB.Exec(fmt.Sprintf("create database %s", cfg.TargetDBCfg.Name))
	if err != nil {
		log.Fatal(err)
	}
	_, err = targetDB.Exec(fmt.Sprintf("use %s", cfg.TargetDBCfg.Name))
	if err != nil {
		log.Fatal(err)
	}

	dailytest.Run(sourceDB, targetDB, cfg.TargetDBCfg.Name, cfg.WorkerCount, cfg.JobCount, cfg.Batch)
}
