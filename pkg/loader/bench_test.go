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

package loader

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func getTestDB() (db *sql.DB, err error) {
	dsn := "root:@tcp(127.0.0.1:3306)/?charset=utf8&interpolateParams=true&readTimeout=1m&multiStatements=true"
	db, err = sql.Open("mysql", dsn)
	return
}

func BenchmarkInsertMerge(b *testing.B) {
	benchmarkWrite(b, true)
}

func BenchmarkInsertNoMerge(b *testing.B) {
	benchmarkWrite(b, false)
}

func BenchmarkUpdateMerge(b *testing.B) {
	benchmarkUpdate(b, true)
}

func BenchmarkUpdateNoMerge(b *testing.B) {
	benchmarkUpdate(b, false)
}

func BenchmarkDeleteMerge(b *testing.B) {
	benchmarkDelete(b, true)
}

func BenchmarkDeleteNoMerge(b *testing.B) {
	benchmarkDelete(b, false)
}

func benchmarkUpdate(b *testing.B, merge bool) {
	r, err := newRunner(merge)
	if err != nil {
		b.Fatal(err)
	}

	if err := dropTable(r.db, r.loader); err != nil {
		b.Fatal(err)
	}
	if err := createTable(r.db, r.loader); err != nil {
		b.Fatal(err)
	}

	if err := loadTable(r.db, r.loader, b.N); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	if err := updateTable(r.db, r.loader, b.N); err != nil {
		b.Fatal(err)
	}

	r.close()
}

func benchmarkDelete(b *testing.B, merge bool) {
	r, err := newRunner(merge)
	if err != nil {
		b.Fatal(err)
	}

	if err := dropTable(r.db, r.loader); err != nil {
		b.Fatal(err)
	}
	if err := createTable(r.db, r.loader); err != nil {
		b.Fatal(err)
	}

	if err := loadTable(r.db, r.loader, b.N); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	deleteTable(r.db, r.loader, b.N)

	r.close()
}

func benchmarkWrite(b *testing.B, merge bool) {
	r, err := newRunner(merge)
	if err != nil {
		b.Fatal(err)
	}

	dropTable(r.db, r.loader)
	createTable(r.db, r.loader)

	b.ResetTimer()
	loadTable(r.db, r.loader, b.N)

	r.close()
}

type runner struct {
	db     *sql.DB
	loader Loader
	wg     sync.WaitGroup
}

func newRunner(merge bool) (r *runner, err error) {
	db, err := getTestDB()
	if err != nil {
		return nil, errors.Trace(err)
	}

	loader, err := NewLoader(db, WorkerCount(16), BatchSize(128))
	if err != nil {
		return nil, errors.Trace(err)
	}

	impl := loader.(*loaderImpl)
	impl.merge = merge

	r = new(runner)
	r.db = db
	r.loader = loader

	r.wg.Add(1)
	go func() {
		err := loader.Run()
		if err != nil {
			log.Fatal("loader run failed", zap.Error(err))
		}
		r.wg.Done()
	}()

	go func() {
		for range loader.Successes() {

		}
	}()

	return
}

func (r *runner) close() {
	r.loader.Close()
	r.wg.Wait()
}

func createTable(db *sql.DB, loader Loader) error {
	sql := "create table test1(id int primary key, a1 int)"
	// sql = "create table test1(id int, a1 int, UNIQUE KEY `id` (`id`))"
	loader.Input() <- NewDDLTxn("test", "test1", sql)

	return nil
}

func dropTable(db *sql.DB, loader Loader) error {
	sql := fmt.Sprintf("drop table if exists test1")
	loader.Input() <- NewDDLTxn("test", "test1", sql)
	return nil
}

func loadTable(db *sql.DB, loader Loader, n int) error {
	var txns []*Txn
	for i := 0; i < n; i++ {
		txn := new(Txn)
		dml := &DML{
			Database: "test",
			Table:    "test1",
			Tp:       InsertDMLType,
			Values: map[string]interface{}{
				"id": i,
				"a1": i,
			},
		}

		txn.AppendDML(dml)
		txns = append(txns, txn)
	}

	for _, txn := range txns {
		loader.Input() <- txn
	}

	return nil
}

func updateTable(db *sql.DB, loader Loader, n int) error {
	var txns []*Txn
	for i := 0; i < n; i++ {
		txn := new(Txn)
		dml := &DML{
			Database: "test",
			Table:    "test1",
			Tp:       UpdateDMLType,
			Values: map[string]interface{}{
				"id": i,
				"a1": i * 10,
			},
			OldValues: map[string]interface{}{
				"id": i,
				"a1": i,
			},
		}

		txn.AppendDML(dml)
		txns = append(txns, txn)
	}

	for _, txn := range txns {
		loader.Input() <- txn
	}

	return nil
}

func deleteTable(db *sql.DB, loader Loader, n int) error {
	var txns []*Txn
	for i := 0; i < n; i++ {
		txn := new(Txn)
		dml := &DML{
			Database: "test",
			Table:    "test1",
			Tp:       DeleteDMLType,
			Values: map[string]interface{}{
				"id": i,
				"a1": i,
			},
		}

		txn.AppendDML(dml)
		txns = append(txns, txn)
	}

	for _, txn := range txns {
		loader.Input() <- txn
	}

	return nil

}
