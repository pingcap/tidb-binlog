package main

import (
	"database/sql"
	"flag"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
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
	b double DEFAULT 2.0, 
	c varchar(10), 
	d time,
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

	// run the simple test case
	dailytest.RunCase(&cfg.DiffConfig, sourceDB, targetDB)

	dailytest.RunTest(&cfg.DiffConfig, sourceDB, targetDB, func(src *sql.DB) {
		// generate insert/update/delete sqls and execute
		dailytest.RunDailyTest(cfg.SourceDBCfg, TableSQLs, cfg.WorkerCount, cfg.JobCount, cfg.Batch)
	})

	dailytest.RunTest(&cfg.DiffConfig, sourceDB, targetDB, func(src *sql.DB) {
		// truncate test data
		dailytest.TruncateTestTable(cfg.SourceDBCfg, TableSQLs)
	})

	dailytest.RunTest(&cfg.DiffConfig, sourceDB, targetDB, func(src *sql.DB) {
		// drop test table
		dailytest.DropTestTable(cfg.SourceDBCfg, TableSQLs)
	})

	log.Info("test pass!!!")
}
