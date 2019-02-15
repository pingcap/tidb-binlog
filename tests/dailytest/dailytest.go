package dailytest

import (
	"database/sql"

	"github.com/ngaut/log"
)

// Run runs the daily test
func Run(sourceDB *sql.DB, targetDB *sql.DB, workerCount int, jobCount int, batch int) {

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

	// run the simple test case
	RunCase(sourceDB, targetDB)

	RunTest(sourceDB, targetDB, func(src *sql.DB) {
		// generate insert/update/delete sqls and execute
		RunDailyTest(sourceDB, TableSQLs, workerCount, jobCount, batch)
	})

	RunTest(sourceDB, targetDB, func(src *sql.DB) {
		// truncate test data
		TruncateTestTable(sourceDB, TableSQLs)
	})

	RunTest(sourceDB, targetDB, func(src *sql.DB) {
		// drop test table
		DropTestTable(sourceDB, TableSQLs)
	})

	log.Info("test pass!!!")

}
