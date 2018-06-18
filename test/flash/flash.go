package main

import (
	"database/sql"
	"flag"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	_ "github.com/kshvakov/clickhouse"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/diff"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	"github.com/pingcap/tidb-binlog/test/dailytest"
	"github.com/pingcap/tidb-binlog/test/util"
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

	targetAddr, err := pkgsql.ParseCHAddr(cfg.TargetDBCfg.Host)
	if err != nil {
		log.Fatal(err)
	}
	if len(targetAddr) != 1 {
		log.Fatal("only support 1 flash node so far.")
	}
	targetDB, err := pkgsql.OpenCH(targetAddr[0].Host, targetAddr[0].Port, cfg.TargetDBCfg.User, cfg.TargetDBCfg.Password, cfg.TargetDBCfg.Name)
	if err != nil {
		log.Fatal(err)
	}
	defer util.CloseDB(targetDB)

	// run the simple test case
	dailytest.RunSimpleCase(sourceDB)

	// wait for sync to downstream sql server
	time.Sleep(60 * time.Second)
	if !CheckSyncState(sourceDB, targetDB) {
		log.Fatal("src don't equal dst")
	}

	// clear the simple test case
	dailytest.ClearSimpleCase(sourceDB)

	// wait for sync to downstream sql server
	time.Sleep(60 * time.Second)
	if !CheckSyncState(sourceDB, targetDB) {
		log.Fatal("src don't equal dst")
	}

	// generate insert/update/delete sqls and execute
	dailytest.RunDailyTest(cfg.SourceDBCfg, TableSQLs, cfg.WorkerCount, cfg.JobCount, cfg.Batch)

	// wait for sync to downstream sql server
	time.Sleep(90 * time.Second)

	// diff the test schema
	if !CheckSyncState(sourceDB, targetDB) {
		log.Fatal("sourceDB don't equal targetDB")
	}

	// truncate test data
	dailytest.TruncateTestTable(cfg.SourceDBCfg, TableSQLs)

	// wait for sync to downstream sql server
	time.Sleep(30 * time.Second)

	// diff the test schema
	if !CheckSyncState(sourceDB, targetDB) {
		log.Fatal("sourceDB don't equal targetDB")
	}

	// drop test table
	dailytest.DropTestTable(cfg.SourceDBCfg, TableSQLs)

	// wait for sync to downstream sql server
	time.Sleep(30 * time.Second)

	// diff the test schema
	if !CheckSyncState(sourceDB, targetDB) {
		log.Fatal("sourceDB don't equal targetDB")
	}

	log.Info("test pass!!!")
}

// CheckSyncState check if srouceDB and targetDB has the same table and data
func CheckSyncState(sourceDB, targetDB *sql.DB) bool {
	d := diff.NewFlash(sourceDB, targetDB)
	ok, err := d.FlashEqual()
	if err != nil {
		log.Fatal(err)
	}

	return ok
}
