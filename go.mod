module github.com/pingcap/tidb-binlog

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/Shopify/sarama v1.24.1
	github.com/dgraph-io/ristretto v0.0.2 // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/fatih/color v1.10.0 // indirect
	github.com/go-openapi/spec v0.20.0 // indirect
	github.com/go-playground/overalls v0.0.0-20191218162659-7df9f728c018 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/mock v1.4.3
	github.com/golang/protobuf v1.3.4
	github.com/google/gofuzz v1.0.0
	github.com/gorilla/mux v1.7.4
	github.com/kami-zh/go-capturer v0.0.0-20171211120116-e492ea43421d
	github.com/mattn/go-colorable v0.1.7 // indirect
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/onsi/ginkgo v1.11.0 // indirect
	github.com/onsi/gomega v1.8.1 // indirect
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pingcap/kvproto v0.0.0-20201215060142-f3dafca4c7fd
	github.com/pingcap/log v0.0.0-20201112100606-8f1e84a3abc8
	github.com/pingcap/parser v0.0.0-20201222091346-02c8ff27d0bc
	github.com/pingcap/tidb v1.1.0-beta.0.20201225085011-3e2ff1d16ce5
	github.com/pingcap/tidb-tools v4.0.9-0.20201127090955-2707c97b3853+incompatible
	github.com/pingcap/tipb v0.0.0-20201209065231-aa39b1b86217
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/client_model v0.2.0
	github.com/rcrowley/go-metrics v0.0.0-20190826022208-cac0b30c2563
	github.com/samuel/go-zookeeper v0.0.0-20170815201139-e6b59f6144be
	github.com/sergi/go-diff v1.1.0 // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726
	github.com/soheilhy/cmux v0.1.4
	github.com/swaggo/swag v1.7.0 // indirect
	github.com/syndtr/goleveldb v1.0.1-0.20190625010220-02440ea7a285
	github.com/tikv/pd v1.1.0-beta.0.20201125070607-d4b90eee0c70
	github.com/unrolled/render v1.0.1
	go.etcd.io/bbolt v1.3.5 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200824191128-ae9734ed278b
	go.uber.org/zap v1.16.0
	golang.org/x/net v0.0.0-20200904194848-62affa334b73
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/sys v0.0.0-20200819171115-d785dc25833f
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/grpc v1.27.1
)

go 1.13

replace github.com/pingcap/tidb => github.com/you06/tidb v1.1.0-beta.0.20201229071942-2be3600223c7
