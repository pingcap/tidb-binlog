package main

import (
	"database/sql"
	"flag"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/diff"
	"github.com/pingcap/tidb-binlog/test/dailytest"
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

	sourceDB, err := util.CreateDB(cfg.SourceDBCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer util.CloseDB(sourceDB)

	targetDB, err := util.CreateDB(cfg.TargetDBCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer util.CloseDB(targetDB)

	// generate insert/update/delete sqls and execute
	dailytest.RunDailyTest(cfg.SourceDBCfg, TableSQLs, cfg.WorkerCount, cfg.JobCount, cfg.Batch)

	// wait for sync to downstream sql server
	time.Sleep(2 * time.Minute)

	// diff the test schema
	if !checkSyncState(sourceDB, targetDB) {
		log.Fatal("sourceDB don't equal targetDB")
	}

	// truncate test data
	dailytest.TruncateTestTable(cfg.SourceDBCfg, TableSQLs)

	// wait for sync to downstream sql server
	time.Sleep(2 * time.Minute)

	// diff the test schema
	if !checkSyncState(sourceDB, targetDB) {
		log.Fatal("sourceDB don't equal targetDB")
	}

	// drop test table
	dailytest.DropTestTable(cfg.SourceDBCfg, TableSQLs)

	// wait for sync to downstream sql server
	time.Sleep(2 * time.Minute)

	// diff the test schema
	if !checkSyncState(sourceDB, targetDB) {
		log.Fatal("sourceDB don't equal targetDB")
	}

	log.Info("test pass!!!")
}

func checkSyncState(sourceDB, targetDB *sql.DB) bool {
	d := diff.New(sourceDB, targetDB)
	ok, err := d.Equal()
	if err != nil {
		log.Fatal(err)
	}

	return ok
}
