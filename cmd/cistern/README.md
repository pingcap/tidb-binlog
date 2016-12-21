## Cistern

cistern collects binlog from each pump in cluster, and store them on disk in order of commitTS.

## How to use

```
Usage of cistern:
  -L string
        log level: debug, info, warn, error, fatal (default "info")
  -addr string
        addr (i.e. 'host:port') to listen on for drainer connections (default "127.0.0.1:8249")
  -collect-interval int
        the interval time (in seconds) of binlog collection loop (default 10)
  -config string
        path to the configuration file
  -data-dir string
        path to the data directory of boltDB (default "data.cistern")
  -gc int
        an integer value to control expiry date of the binlog data, indicates for how long (in days) the binlog data would be stored. default value is 0, means binlog data would never be removed.
  -log-file string
        log file path
  -log-rotate string
        log file rotate type, hour/day
  -metrics-addr string
        prometheus pushgateway address, leaves it empty will disable prometheus push.
  -metrics-interval int
        prometheus client push interval in second, set "0" to disable prometheus push. (default 15)
  -pd-urls string
        a comma separated list of PD endpoints (default "http://127.0.0.1:2379")
  -version
        print version info
```


## Example

```
./bin/pump -socket unix:///tmp/pump.sock \
           -pd-urls http://127.0.0.1:2379 \
           -data-dir data.pump
```
or use configuration file

```
./bin/cistern -config ./conf/cistern.toml
```
