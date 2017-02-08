## drainer

drainer collects binlog from each pump in cluster, and apply to downstream database or filesystem.

## How to use

```
Usage of drainer:
  -L string
        log level: debug, info, warn, error, fatal (default "info")
  -addr string
        addr (i.e. 'host:port') to listen on for drainer connections (default "127.0.0.1:8249")
  -c int
        parallel worker count (default 1)
  -config string
        path to the configuration file
  -data-dir string
        drainer data directory path (default data.drainer)
  -db-host string
        host of target database (default "127.0.0.1")
  -db-password string
        password of target database
  -db-port int
        port of target database (default 3306)
  -db-username string
        username of target database (default "root")
  -dest-db-type string
        target db type: mysql, postgresql (default "mysql")
  -detect-interval int
        the interval time (in seconds) of detect pumps' status (default 10)
  -ignore-schemas string
        disable sync the meta schema (default "INFORMATION_SCHEMA,PERFORMANCE_SCHEMA,mysql")
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
  -txn-batch int
        number of binlog events in a transaction batch (default 1)
  -version
        print version info
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
