package dailytest

import (
	"sync"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/test/util"
)

// Run execute the test case
func Run(dbCfg util.DBConfig, tableSQLs []string, workerCount int, jobCount int, batch int) {
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

			dbs, err := createDBs(dbCfg, workerCount)
			if err != nil {
				log.Fatal(err)
			}
			defer closeDBs(dbs)

			err = execSQL(dbs[0], tableSQLs[i])
			if err != nil {
				log.Fatal(err)
			}

			doProcess(table, dbs, jobCount, workerCount, batch)
		}(i)
	}

	wg.Wait()
}
