## TiDB-Binlog

A commercial tool used to collect and merge [TiDB's](https://github.com/pingcap/tidb) binlog for real-time data backup and synchronization.


## How to build

```
make build   # build all tools

make pump    # build pump

make cistern # build cistern

make drainer # build drainer
```

When build successfully, you can find the binary in bin directory. 

## Service list

[pump](./cmd/pump)

A service that provoids rpc interfces that write binlog items and pull binlog items.

[cistern](./cmd/cistern)

A service that pulls binlog items from all pump, then sorts them and generates TiDB-Binlogs by commitTs

[drainer](./cmd/drainer)

A tool that queries TiDB-Binlogs from cistern, then executes them on target DB.

## Deployment

TiDB-binlog contains pump, cistern and drainer services, the boot sequence is related to PD, TiDB, TiKV。

The recommended startup sequence: PD -> TiKV -> [pump](./cmd/pump) -> TiDB -> [cistern](./cmd/cistern) -> [drainer](./cmd/drainer)