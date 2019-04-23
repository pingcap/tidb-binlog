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

import "log"

func Example() {
	// create sql.DB
	db, err := CreateDB("root", "", "localhost", 4000)
	if err != nil {
		log.Fatal(err)
	}

	// init loader
	loader, err := NewLoader(db, WorkerCount(16), BatchSize(128))
	if err != nil {
		log.Fatal(err)
	}

	// get the success txn  from loader
	go func() {
		// the return order will be the order you push into loader.Input()
		for txn := range loader.Successes() {
			log.Print("succ: ", txn)
		}
	}()

	// run loader
	go func() {
		// return non nil if encounter some case fail to load data the downstream
		// or nil when loader is closed when all data is loaded to downstream
		err := loader.Run()
		if err != nil {
			log.Fatal(err)
		}
	}()

	// push ddl txn
	loader.Input() <- NewDDLTxn("test", "test", "create table test(id primary key)")

	// push one insert dml txn
	values := map[string]interface{}{"id": 1}
	loader.Input() <- &Txn{
		DMLs: []*DML{{Database: "test", Table: "test", Tp: InsertDMLType, Values: values}},
	}

	// push one update dml txn
	newValues := map[string]interface{}{"id": 2}
	loader.Input() <- &Txn{
		DMLs: []*DML{{Database: "test", Table: "test", Tp: UpdateDMLType, Values: newValues, OldValues: values}},
	}

	// you can set safe mode or not at run time
	// which use replace for insert event and delete + replace for update make it be idempotent
	loader.SetSafeMode(true)

	// push one delete dml txn
	loader.Input() <- &Txn{
		DMLs: []*DML{{Database: "test", Table: "test", Tp: DeleteDMLType, Values: newValues}},
	}
	//...

	// Close the Loader. No more Txn can be push into Input()
	// Run will quit when all data is drained
	loader.Close()
}
