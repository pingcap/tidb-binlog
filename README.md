## TiDB-Binlog

A commercial tool used to collect [TiDB's](https://github.com/pingcap/tidb) binlog, with features:

* *Data replication*: synchronize datas from TiDB cluster to heterogeneous databases.
* *Real-time backup & recovery*: backup TiDB cluster into dump file and can be used for recovery.
* *Multiple output format*: support mysql, dump file, etc.
* *history replay*: replay from any history point.

## Documentation

+ [简体中文](./docs/doc-cn.md)

## Architecture

![architecture](./docs/architecture.jpeg)

## How to build

```
make build   # build all compoents

make pump    # build pump

make cistern # build cistern

make drainer # build drainer
```

When build successfully, you can find the binary in bin directory. 

## Service list

[pump](./cmd/pump)

pump is a daemon that receives realtime binlog from tidb-server and writes in sequential disk files synchronously.

[cistern](./cmd/cistern)

cistern collects binlog from each pump in cluster, and store them on disk in order of commitTS.

[drainer](./cmd/drainer)

drainer transforms binlog to various dialects of SQL, and apply to downstream database or filesystem.

## Deployment

The recommended startup sequence: PD -> TiKV -> [pump](./cmd/pump) -> TiDB -> [cistern](./cmd/cistern) -> [drainer](./cmd/drainer)