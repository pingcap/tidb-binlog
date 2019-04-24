## drainer

drainer collects binlog from each pump in cluster, transforms binlog to various dialects of SQL, and applies to downstream database or filesystem.

## How to use

```
Usage of drainer:
  -L string
      log level: debug, info, warn, error, fatal (default "info")
  -V  print version info
  -addr string
      addr (i.e. 'host:port') to listen on for drainer connections (default "127.0.0.1:8249")
  -c int
      parallel worker count (default 1)
  -cache-binlog-count int
      blurry count of binlogs in cache, limit cache size (default 65536)
  -config string
      path to the configuration file
  -data-dir string
      drainer data directory path (default data.drainer) (default "data.drainer")
  -dest-db-type string
      target db type: mysql or tidb or file or kafka; see syncer section in conf/drainer.toml (default "mysql")
  -detect-interval int
      the interval time (in seconds) of detect pumps' status (default 10)
  -disable-detect
      disbale detect causality
  -disable-dispatch
      disable dispatching sqls that in one same binlog; if set true, work-count and txn-batch would be useless
  -ignore-schemas string
      disable sync those schemas (default "INFORMATION_SCHEMA,PERFORMANCE_SCHEMA,mysql")
  -initial-commit-ts int
      if drainer donesn't have checkpoint, use initial commitTS to initial checkpoint
  -kafka-addrs string
      a comma separated list of the kafka broker endpoints (default "127.0.0.1:9092")
  -log-file string
      log file path
  -log-rotate string
      log file rotate type, hour/day
  -metrics-addr string
      prometheus pushgateway address, leaves it empty will disable prometheus push
  -metrics-interval int
      prometheus client push interval in second, set "0" to disable prometheus push (default 15)
  -pd-urls string
      a comma separated list of PD endpoints (default "http://127.0.0.1:2379")
  -safe-mode
      enable safe mode to make syncer reentrant
  -txn-batch int
      number of binlog events in a transaction batch (default 1)
  -zookeeper-addrs string
      a comma separated list of the zookeeper endpoints
```


## Example

```
./bin/drainer -pd-urls http://127.0.0.1:2379 \
              -data-dir ./data.drainer
```
or use configuration file

```
./bin/drainer -config ./conf/drainer.toml
```
