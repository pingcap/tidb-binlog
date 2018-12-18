package dailytest

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/ngaut/log"
)

// RunDailyTest generates insert/update/delete sqls and execute
func RunDailyTest(db *sql.DB, tableSQLs []string, workerCount int, jobCount int, batch int) {
	var wg sync.WaitGroup
	wg.Add(len(tableSQLs))

	for i := range tableSQLs {
		go func(i int) {
			defer wg.Done()

			table := newTable()
			err := parseTableSQL(table, tableSQLs[i])
			if err != nil {
				log.Fatal(err)
			}

			err = execSQL(db, tableSQLs[i])
			if err != nil {
				log.Fatal(err)
			}

			doProcess(table, db, jobCount, workerCount, batch)
		}(i)
	}

	wg.Wait()
}

// TruncateTestTable truncates test data
func TruncateTestTable(db *sql.DB, tableSQLs []string) {
	for i := range tableSQLs {
		table := newTable()
		err := parseTableSQL(table, tableSQLs[i])
		if err != nil {
			log.Fatal(err)
		}

		err = execSQL(db, fmt.Sprintf("truncate table %s", table.name))
		if err != nil {
			log.Fatal(err)
		}
	}
}

// DropTestTable drops test table
func DropTestTable(db *sql.DB, tableSQLs []string) {
	for i := range tableSQLs {
		table := newTable()
		err := parseTableSQL(table, tableSQLs[i])
		if err != nil {
			log.Fatal(err)
		}

		err = execSQL(db, fmt.Sprintf("drop table %s", table.name))
		if err != nil {
			log.Fatal(err)
		}
	}
}
