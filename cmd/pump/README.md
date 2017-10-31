## pump

pump is a daemon that receives realtime binlog from tidb-server and writes in sequential disk files synchronously.

## How to use

```
 Usage of pump:
  -L string
        log level: debug, info, warn, error, fatal (default "info")
  -addr string
        addr(i.e. 'host:port') to listen on for client traffic (default "127.0.0.1:8250")
  -advertise-addr string
        addr(i.e. 'host:port') to advertise to the public
  -config string
        path to the pump configuration file
  -data-dir string
        the path to store binlog data
  -kafka-addrs string 
	kafka where drainer pull binlog from (default "127.0.0.1:9092")
  -gc int
        recycle binlog files older than gc days, zero means never recycle (default 7)
  -heartbeat-interval int
        number of seconds between heartbeat ticks (default 2)
  -log-file string
        log file path
  -log-rotate string
        log file rotate type, hour/day
  -metrics-addr string
        prometheus pushgateway address, leaves it empty will disable prometheus push.
  -metrics-interval int
        prometheus client push interval in second, set "0" to disable prometheus push. (default 15)
  -node-id string
        the ID of pump node; if not specify, we will generate one from hostname and the listening port
  -pd-urls string
        a comma separated list of the PD endpoints (default "http://127.0.0.1:2379")
  -socket string
        unix socket addr to listen on for client traffic
  -version
        print pump version info
```


## Example

```
./bin/pump -socket unix:///tmp/pump.sock \
           -pd-urls http://127.0.0.1:2379 \
           -data-dir ./data.pump
```
or use configuration file

```
./bin/pump -config ./conf/pump.toml
```

## Deployment
You should deployment pump server for each TiDB node in the cluster, pump can serve tidb-servers across different clusters.
