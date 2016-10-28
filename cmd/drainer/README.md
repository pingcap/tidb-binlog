## Drainer

A tool that queries TiDB-Binlogs from cistern, then executes them on target DB.

## How to use

```
Usage of drainer:
  -L string
       	log level: debug, info, warn, error, fatal (default "info")
  -config string
       	Config file
  -data-dir string
       	drainer data directory path (default "data.drainer")
  -dest-db-type string
       	to db type: Mysql, PostgreSQL (default "mysql")
  -ignore-schemas string
       	disable sync the meta schema (default "INFORMATION_SCHEMA,PERFORMANCE_SCHEMA,mysql,test")
  -init-commit-ts int
       	the init point for sync
  -log-file string
       	log file path
  -log-rotate string
       	log file rotate type, hour/day
  -metrics-addr string
       	prometheus pushgateway address, leaves it empty will disable prometheus push.
  -metrics-interval int
       	prometheus client push interval in second, set "0" to disable prometheus push. (default 15)
  -pprof-addr string
       	pprof addr (default ":10081")
  -txn-batch int
       	number of binlog events in a transaction batch (default 1)
```


## Example

```
./bin/drainer -dest-db-type mysql -ignore-schemas INFORMATION_SCHEMA,PERFORMANCE_SCHEMA,mysql -data-dir data.drainer
```
or use toml file

```
./bin/drainer -config-file config.toml
```

## Precautions
Currently drainer only supports mysql