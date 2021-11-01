module github.com/pingcap/tidb-binlog

go 1.16

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/Shopify/sarama v1.30.0
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/google/btree v1.0.1 // indirect
	github.com/google/gofuzz v1.0.0
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kami-zh/go-capturer v0.0.0-20171211120116-e492ea43421d
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20210425183316-da1aaba5fb63
	github.com/pingcap/failpoint v0.0.0-20210918120811-547c13e3eb00 // indirect
	github.com/pingcap/kvproto v0.0.0-20211011042309-a4518fcacbc8
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/tidb v1.1.0-beta.0.20211026030648-c497d5c06348
	github.com/pingcap/tidb-tools v5.2.2-0.20211019062242-37a8bef2fa17+incompatible
	github.com/pingcap/tidb/parser v0.0.0-20211026030648-c497d5c06348
	github.com/pingcap/tipb v0.0.0-20211026080602-ec68283c1735
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/samuel/go-zookeeper v0.0.0-20201211165307-7117e9ea2414
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cobra v1.2.1 // indirect
	github.com/syndtr/goleveldb v1.0.1-0.20190318030020-c3a204f8e965
	github.com/tikv/client-go/v2 v2.0.0-alpha.0.20211028082558-c4250227823e
	github.com/tikv/pd v1.1.0-beta.0.20211027071649-433d4f2847be
	github.com/unrolled/render v1.0.1
	go.etcd.io/etcd v0.5.0-alpha.5.0.20210512015243-d19fbe541bf9
	go.uber.org/zap v1.19.1
	golang.org/x/net v0.0.0-20211020060615-d418f374d309
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20211025201205-69cdffdb9359
	google.golang.org/grpc v1.41.0
	sigs.k8s.io/yaml v1.3.0 // indirect
)

replace google.golang.org/grpc => google.golang.org/grpc v1.29.1

replace github.com/pingcap/tidb-tools => github.com/rleungx/tidb-tools v5.2.3-0.20211101050842-23301ee7b003+incompatible
