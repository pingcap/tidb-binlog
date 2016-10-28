## Pump

A service that provoids rpc interfces that write binlog items and pull binlog items.

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
./bin/pump -socket unix:///tmp/pump.sock -pd-urls http://127.0.0.1:2379 (unix socket style)
```
or use toml file

```
./bin/pump -config-file config.toml
```

## Deployment
You should deployment pump server for every TiDB in the cluster, more TiDBs that are not in same cluster can share one pump.
