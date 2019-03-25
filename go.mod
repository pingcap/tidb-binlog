module github.com/pingcap/tidb-binlog

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.3.2
	github.com/Shopify/sarama v1.18.0
	github.com/Shopify/toxiproxy v2.1.3+incompatible // indirect
	github.com/cloudfoundry-incubator/candiedyaml v0.0.0-20160429080125-99c3df83b515 // indirect
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/coreos/etcd v3.3.0-rc.0.0.20180530235116-2b3aa7e1d49d+incompatible
	github.com/cznic/golex v0.0.0-20160422121650-da5a7153a510 // indirect
	github.com/cznic/parser v0.0.0-20160622100904-31edd927e5b1 // indirect
	github.com/cznic/strutil v0.0.0-20150430124730-1eb03e3cc9d3 // indirect
	github.com/cznic/y v0.0.0-20160420101755-9fdf92d4aac0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/eapache/go-resiliency v1.1.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v0.0.0-20180227141424-093482f3f8ce // indirect
	github.com/go-sql-driver/mysql v1.4.0
	github.com/gogo/protobuf v1.1.1
	github.com/golang-collections/collections v0.0.0-20130729185459-604e922904d3 // indirect
	github.com/golang/lint v0.0.0-20181217174547-8f45f776aaf1 // indirect
	github.com/golang/protobuf v1.1.0
	github.com/google/gofuzz v0.0.0-20170612174753-24818f796faf
	github.com/gorilla/context v1.1.1 // indirect
	github.com/gorilla/mux v1.6.2
	github.com/gorilla/websocket v1.4.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v0.0.0-20180820150422-93bf4626fba7 // indirect
	github.com/juju/loggo v0.0.0-20180524022052-584905176618 // indirect
	github.com/juju/testing v0.0.0-20180920084828-472a3e8b2073 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/kshvakov/clickhouse v1.3.5 // indirect
	github.com/montanaflynn/stats v0.0.0-20180911141734-db72e6cae808 // indirect
	github.com/ngaut/deadline v0.0.0-20150302045450-fae8f9dfd704 // indirect
	github.com/ngaut/log v0.0.0-20160810023011-cec23d3e10b0
	github.com/onsi/gomega v1.4.1
	github.com/petar/GoLLRB v0.0.0-20130427215148-53be0d36a84c // indirect
	github.com/pierrec/lz4 v2.0.5+incompatible // indirect
	github.com/pingcap/check v0.0.0-20171206051426-1c287c953996
	github.com/pingcap/errors v0.11.0
	github.com/pingcap/parser v0.0.0-20190118033454-a52e5bde3bd2
	github.com/pingcap/pd v2.1.3+incompatible
	github.com/pingcap/tidb v2.1.3+incompatible
	github.com/pingcap/tidb-tools v2.1.5+incompatible
	github.com/pingcap/tipb v0.0.0-20190107072121-abbec73437b7
	github.com/prometheus/client_golang v0.8.0
	github.com/rcrowley/go-metrics v0.0.0-20180503174638-e2704e165165
	github.com/samuel/go-zookeeper v0.0.0-20170815201139-e6b59f6144be
	github.com/siddontang/go v0.0.0-20161005110831-1e9ce2a5ac40
	github.com/sirupsen/logrus v0.0.0-20180830201151-78fa2915c1fa // indirect
	github.com/soheilhy/cmux v0.1.2
	github.com/spaolacci/murmur3 v0.0.0-20180118202830-f09979ecbc72 // indirect
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/syndtr/goleveldb v0.0.0-20180708030551-c4c61651e9e3
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/uber-go/atomic v1.3.2 // indirect
	github.com/unrolled/render v0.0.0-20180807193321-4206df6ff701
	github.com/urfave/negroni v1.0.0 // indirect
	github.com/zanmato1984/clickhouse v1.3.4-0.20181106115746-3e9a6b9beb12
	go.uber.org/atomic v1.3.2 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.9.1 // indirect
	golang.org/x/lint v0.0.0-20181011164241-5906bd5c48cd // indirect
	golang.org/x/net v0.0.0-20180906233101-161cd47e91fd
	golang.org/x/sync v0.0.0-20180314180146-1d60e4601c6f
	golang.org/x/sys v0.0.0-20180909124046-d0be0721c37e
	golang.org/x/tools v0.0.0-20181012201414-c0eb142035b5 // indirect
	google.golang.org/appengine v0.0.0-20180731164958-4216e58b9158 // indirect
	google.golang.org/genproto v0.0.0-20181004005441-af9cb2a35e7f // indirect
	google.golang.org/grpc v1.12.0
	gopkg.in/airbrake/gobrake.v2 v2.0.9 // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/gemnasium/logrus-airbrake-hook.v2 v2.1.2 // indirect
	gopkg.in/mgo.v2 v2.0.0-20180705113604-9856a29383ce // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0-20170531160350-a96e63847dc3 // indirect
)
