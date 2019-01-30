Arbiter
==========

**Arbiter** 是一个从 Kafka 获取 Binlog 增量同步数据到 TiDB 的工具.

![](./arbiter.png)

整体工作原理如下：

1. 读取 Kafka 的 [Protobuf](https://github.com/pingcap/tidb-tools/blob/master/tidb-binlog/slave_binlog_proto/proto/binlog.proto) 格式 Binlog 。
2. 达到一定数据量后 根据 Binlog 构造对应 SQL 并发写入下游（注意 Arbiter 会拆分上游事务)。
3. 保存 checkpoint 。


## Checkpoint
`arbiter` 会在下游 TiDB `tidb_binlog.arbiter_checkpoint` 表里保存一条 checkpoint 记录。
```
mysql> select * from tidb_binlog.arbiter_checkpoint;
+-------------+--------------------+--------+
| topic_name  | ts                 | status |
+-------------+--------------------+--------+
| test_kafka4 | 405809779094585347 |      1 |
+-------------+--------------------+--------+
```
- topic_name: 消费的 Kafka 主题名。
- ts: 当前同步到了哪个 ts
- status:
	* 0
	表示 <= ts 的数据都同步到下游了。
	* 1
	运行中或者异常退出，> ts 后的部分 Binlog 可能同步到下游。



## 监控

Arbiter 支持给 [Prometheus](https://prometheus.io/) 采集度量 (metrics)。

### 度量

* **`binlog_arbiter_checkpoint_tso`** (测量仪)

	对应 `tidb_binlog.arbiter_checkpoint` 表里的 ts

* **`binlog_arbiter_query_duration_time`** (直方图)

	写下游需时的直方图。标签:

	* **type**: `exec` `commit` 执行 SQL 跟提交时的耗时。

* **`binlog_arbiter_event`** (计数器)

	计算事件次数

	* **type**: `DDL` `Insert` `Update` `Delete` `Txn`

* **`binlog_arbiter_queue_size`** (测量仪)

	内部队列数据囤积大小。标签：
	
	* **name**: `kafka_reader` `loader_input`

* **`binlog_arbiter_txn_latency_seconds`** (直方图)

	上游事务提交(commitTS物理时间) 到对应事务写入下游的花时。


