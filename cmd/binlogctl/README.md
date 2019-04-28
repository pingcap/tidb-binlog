# binlogctl

binlogctl is a tool for performing some tidb-binlog related operations, like querying the status of Pump/Drainer and unregistering some Pump/Drainer.

## How to use

```
Usage of binlogctl:
	-V	prints version and exit
	-cmd string
		operator: "generate_meta", "pumps", "drainers", "update-pump", "update-drainer", "pause-pump", "pause-drainer", "offline-pump", "offline-drainer" (default "pumps")
	-data-dir string
		meta directory path (default "binlog_position")
	-node-id string
		id of node, used to delete some node with operations delete-pump and delete-drainer
	-pd-urls string
		a comma separated list of PD endpoints (default "http://127.0.0.1:2379")
	-ssl-ca string
		Path of file that contains the list of trusted SSL CAs for connection with cluster components
	-ssl-cert string
		Path of file that contains X509 certificate in PEM format for connection with cluster components
	-ssl-key string
		Path of file that contains X509 key in PEM format for connection with cluster components
	-time-zone Asia/Shanghai
		set time zone if you want to save time info in the savepoint file, for example `Asia/Shanghai` for CST time and `Local` for the local time
```

## Example

### Query Pump/Drainer

Run the following command:

```
bin/binlogctl -pd-urls=http://127.0.0.1:2379 -cmd pumps
bin/binlogctl -pd-urls=http://127.0.0.1:2379 -cmd drainers
```

Then the result will be like this (the output will be formatted later):

```
[2019/04/28 09:29:59.016 +00:00] [INFO] [nodes.go:48] ["query node"] [type=pump] [node="{NodeID: 1.1.1.1:8250, Addr: pump:8250, State: online, MaxCommitTS: 408012403141509121, UpdateTime: 2019-04-28 09:29:57 +0000 UTC}"]
```

### Unregister Pump/Drainer

### update pump/drainer's state
pump/drainer's state can be online, pausing, paused, closing and offline. In most cases, we only need update pump/drainer's state to paused or offline.
```
bin/binlogctl -pd-urls=http://127.0.0.1:2379 -cmd update-pump/update-drainer -node-id ip-127-0-0-1:8250/{nodeID} -state {state}
```
This cmd will update pump/drainer's state.

### pause/offline pump/drainer
```
bin/binlogctl -pd-urls=http://127.0.0.1:2379 -cmd pause-pump/pause-drainer/offline-pump/offline-drainer -node-id ip-127-0-0-1:8250/{nodeID}
```
binlogctl will send http request to pump/drainer, and finally pump/drainer will exit by itself with paused or offline state.

### Generate `meta`

`meta` contains commit TS that can be used to specify the location of the synchronized data.

Run the following command:

```
bin/binlogctl -pd-urls=http://127.0.0.1:2379 -cmd generate_meta
```

Then the result will be like this:

```
INFO[0000] [pd] create pd client with endpoints [http://192.168.199.118:32379]
INFO[0000] [pd] leader switches to: http://192.168.199.118:32379, previous:
INFO[0000] [pd] init cluster id 6569368151110378289
[2019/04/28 09:33:15.950 +00:00] [INFO] [meta.go:114] ["save meta"] [meta="commitTS: 408012454863044609"]
```

It will also generate a `{data-dir}/savepoint` meta file

TODO: improve `meta` later, like adding offset of the Kafka topic that corresponds to each Pump node

## License

Apache 2.0 license. See the [LICENSE](../LICENSE) file for details.
