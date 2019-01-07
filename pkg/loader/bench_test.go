package loader

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
)

func getTestDB() (db *sql.DB, err error) {
	dsn := "root:@tcp(127.0.0.1:3306)/?charset=utf8&interpolateParams=true&readTimeout=1m&multiStatements=true"
	db, err = sql.Open("mysql", dsn)
	return
}

func BenchmarkInsertKey(b *testing.B) {
	benchmarkWrite(b, true)
}

func BenchmarkInsertNoKey(b *testing.B) {
	benchmarkWrite(b, false)
}

func benchmarkWrite(b *testing.B, key bool) {
	log.SetLevelByString("info")

	db, err := getTestDB()
	if err != nil {
		b.Fatal(err)
	}

	loader, err := NewLoader(db, 16, 128)
	if err != nil {
		b.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err := loader.Run()
		if err != nil {
			log.Error(err)
			b.Fail()
		}
		wg.Done()
	}()

	go func() {
		for range loader.Successes() {

		}
	}()

	sql := fmt.Sprintf("drop table if exists test1")
	loader.Input() <- NewDDLTxn("test", "test", sql)

	var keyStr string
	if key {
		keyStr = "primary key"
	}
	sql = fmt.Sprintf("create table test1(id int %s, a1 int)", keyStr)
	loader.Input() <- NewDDLTxn("test", "test", sql)

	var txns []*Txn
	for i := 0; i < b.N; i++ {
		txn := new(Txn)
		dml := new(DML)
		dml.Database = "test"
		dml.Table = "test1"
		dml.Tp = InsertDMLType
		dml.Values = make(map[string]interface{})
		dml.Values["id"] = i
		dml.Values["a1"] = i

		txn.AppendDML(dml)
		txns = append(txns, txn)
	}

	for _, txn := range txns {
		loader.Input() <- txn
	}

	loader.Close()
	wg.Wait()
}
