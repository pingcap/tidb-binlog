package main

import (
	"flag"
	"os"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/test/dailytest"
	"github.com/pingcap/tidb-binlog/test/diff"
	"github.com/pingcap/tidb-binlog/test/util"
)

func main() {
	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Errorf("parse cmd flags err %s\n", err)
		os.Exit(2)
	}

	TableSQLs := []string{`
create table ptest(
	a int primary key, 
	b double NOT NULL DEFAULT 2.0, 
	c varchar(10) NOT NULL, 
	d time unique
);
`,
		`
create table itest(
	a int, 
	b double NOT NULL DEFAULT 2.0, 
	c varchar(10) NOT NULL, 
	d time unique,
	PRIMARY KEY(a, b)
);
`,
		`
create table ntest(
	a int, 
	b double NOT NULL DEFAULT 2.0, 
	c varchar(10) NOT NULL, 
	d time unique
);
`}

	// generate sql and execute
	dailytest.Run(cfg.SourceDBCfg, TableSQLs, cfg.WorkerCount, cfg.JobCount, cfg.Batch)

	time.Sleep(3 * time.Minute)

	sourceDB, err := util.CreateDB(cfg.SourceDBCfg)
	if err != nil {
		log.Fatal(err)
	}
	targetDB, err := util.CreateDB(cfg.TargetDBCfg)
	if err != nil {
		log.Fatal(err)
	}

	d := diff.New(sourceDB, targetDB)
	ok, err := d.Equal()
	if err != nil {
		log.Fatal(err)
	}

	if !ok {
		log.Fatal("sourceDB don't equal targetDB")
	}

	log.Info("test pass!!!")
}
