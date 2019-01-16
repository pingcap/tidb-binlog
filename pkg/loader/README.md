loader
======

A package to load data into MySQL in real-time, aimed to be used by *reparo*, *drainer* etc unified.


### Getting started
- Example is available via [example_loader_test.go](./example_loader_test.go)

	you need to write a translater to use *Loader* like *SlaveBinlogToTxn* in [translate.go](./translate.go)


## Overview
Loader will split the upstream transaction DML events and concurrently(shared by primary key or unique key) load data to mysql, it will solve the causality by [causality.go](./causality.go).


## Optimization
#### Large Operation
Instead of execute DML one by one, we can combine many small operations into a single large operation like use INSERT statements with multiple VALUES lists to insert several rows at a time, this may get [high-speed](https://medium.com/@benmorel/high-speed-inserts-with-mysql-9d3dcd76f723) compare to insert one by one.

#### Merge by Primary Key
You may want to read [log-compaction](https://kafka.apache.org/documentation/#compaction) of kafka.

Let's say for a table with Primary Key, we can treat it like a KV-store, to reload the table with the change history of table, we only need the last value for every key. 

While syncing data into downstream at real-time, we can get DML events from upstream in batch and merge by key, after merge, there's only one event for one key, at downstream, we don't need doing as many events as upstream, this also help we to use batch insert operation.

 We should consider secondary unique key here, see *execTableBatch* in [executor.go](./executor.go). currently, we only merge by primary key and do batch operation if the table have primary key and no unique key.



