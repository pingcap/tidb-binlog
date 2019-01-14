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
		DMLs: []*DML{{Database: "test", Table: "test", Tp: InsertDMLType, Values: newValues, OldValues: values}},
	}

	// push one delete dml txn
	loader.Input() <- &Txn{
		DMLs: []*DML{{Database: "test", Table: "test", Tp: InsertDMLType, Values: newValues}},
	}
	//...

	// Close close the Loader, no more Txn can be push into Input())
	// Run will quit when all data is drained
	loader.Close()
}
