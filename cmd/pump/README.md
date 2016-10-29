## Pump

pump is a daemon that receives realtime binlog from tidb-server and writes in sequential disk files synchronously.

## How to use

```
Usage of pump:
  -addr string
       	addr(i.e. 'host:port') to listen on for client traffic (default "127.0.0.1:8250")
  -advertise-addr string
       	addr(i.e. 'host:port') to advertise to the public
  -config-file string
       	path to the pump configuration file
  -data-dir string
       	the path to store binlog data
  -debug
       	whether to enable debug-level logging
  -gc uint
       	recycle binlog files older than gc days, zero means never recycle (default 7)
  -heartbeat-interval uint
       	number of seconds between heartbeat ticks (default 2)
  -pd-urls string
       	a comma separated list of the PD endpoints (default "http://127.0.0.1:2379")
  -socket string
       	unix socket addr to listen on for client traffic
  -version
       	print pump version info
```


## Example

```
./bin/pump -socket unix:///tmp/pump.sock -pd-urls http://127.0.0.1:2379
```
or use configuration file

```
./bin/pump -config-file config.toml
```

## Deployment
You should deployment pump server for each TiDB node in the cluster, pump can serve tidb-servers across different clusters.
