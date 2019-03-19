Arbiter
==========

**Arbiter** is a tool used for syncing data from Kafka to TiDB incrementally.

![](./arbiter.png)

The complete import process is as follows:

1. Read Binlog from Kafka in the format of [Protobuf](https://github.com/pingcap/tidb-tools/blob/master/tidb-binlog/slave_binlog_proto/proto/binlog.proto).
2. While reaching a limit data size, construct the SQL according the Binlog and write to downstream concurrently(notice: Arbiter will split the upstream transaction).
3. Save the checkpoint.


## Checkpoint
`arbiter` will write a record to the table `tidb_binlog.arbiter_checkpoint` at downstream TiDB.
```
mysql> select * from tidb_binlog.arbiter_checkpoint;
+-------------+--------------------+--------+
| topic_name  | ts                 | status |
+-------------+--------------------+--------+
| test_kafka4 | 405809779094585347 |      1 |
+-------------+--------------------+--------+
```
- topic_name: the topic name of Kafka to consume.
- ts: the timestamp checkpoint
- status:
	* 0
	All Binlog data <= ts has synced to downstream.
	* 1
	means `Arbiter` is running or quit unexpectedly, Binlog with timestamp bigger than ts may partially synced to downstream.



## Monitor

Arbiter supports metrics collection via [Prometheus](https://prometheus.io/).

###Metrics

* **`binlog_arbiter_checkpoint_tso`** (Gauge)

	Corresponding to ts in table `tidb_binlog.arbiter_checkpoint`

* **`binlog_arbiter_query_duration_time`** (Histogram)

	Bucketed histogram of the time needed to wirte to downstream. Labels:

	* **type**: `exec` `commit` time takes to execute and commit SQL.

* **`binlog_arbiter_event`** (Counter)

	Event times counter. Labels:

	* **type**: e.g. `DDL` `Insert` `Update` `Delete` `Txn`

* **`binlog_arbiter_queue_size`** (Gauge)

	Queue size. Labels:
	
	* **name**: e.g. `kafka_reader` `loader_input`

* **`binlog_arbiter_txn_latency_seconds`** (Histogram)

	Bucketed histogram of the time duration between the time write to downstream and commit time of upstream transaction(phsical part of commitTS).


