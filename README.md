# TiDB-Binlog

## TiDB-Binlog introduction

TiDB-Binlog is a commercial tool used to collect [TiDB's](https://github.com/pingcap/tidb) binary logs with the following features:

- Data replication
    
    Synchronize data from the TiDB cluster to heterogeneous databases.

- Real-time backup and recovery
    
    Backup the TiDB cluster into the Dump file and it can be used for recovery.

- Multiple output format
    
    Support MySQL, Dump file, etc.
    
- History replay
    
    Replay from any history point.

## Documentation

+ [简体中文](https://github.com/pingcap/docs-cn/blob/master/tools/tidb-binlog-cluster.md)

## Architecture

![architecture](./docs/architecture.png)

## How to build

```
make build   # build all components

make pump    # build pump

make drainer # build drainer
```

When TiDB-Binlog is built successfully, you can find the binary in the `bin` directory. 

## Service list

[Pump](./cmd/pump)

Pump is a daemon that receives real-time binlogs from tidb-server and writes in sequential disk files synchronously.

[Drainer](./cmd/drainer)

Drainer collects binlogs from each Pump in the cluster, transforms binlogs to various dialects of SQL, and applies to the downstream database or filesystem.

## Deployment

The recommended startup sequence: PD -> TiKV -> [Pump](./cmd/pump) -> TiDB -> [Drainer](./cmd/drainer)
