module github.com/pingcap/tidb-binlog

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.3.0
	github.com/Shopify/sarama v1.24.1
	github.com/dustin/go-humanize v1.0.0
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/mock v1.3.1
	github.com/golang/protobuf v1.3.4
	github.com/google/gofuzz v1.0.0
	github.com/gorilla/mux v1.7.3
	github.com/kami-zh/go-capturer v0.0.0-20171211120116-e492ea43421d
	github.com/mattn/go-colorable v0.1.7 // indirect
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20200917111840-a15ef68f753d
	github.com/pingcap/kvproto v0.0.0-20200916031750-f9473f2c5379
	github.com/pingcap/log v0.0.0-20200828042413-fce0951f1463
	github.com/pingcap/parser v0.0.0-20200921041333-cd2542b7a8a2
	github.com/pingcap/tidb v1.1.0-beta.0.20200922113008-462927bf31f4
	github.com/pingcap/tidb-tools v4.0.5-0.20200820092506-34ea90c93237+incompatible
	github.com/pingcap/tipb v0.0.0-20200618092958-4fad48b4c8c3
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/client_model v0.2.0
	github.com/rcrowley/go-metrics v0.0.0-20181016184325-3113b8401b8a
	github.com/samuel/go-zookeeper v0.0.0-20170815201139-e6b59f6144be
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726
	github.com/soheilhy/cmux v0.1.4
	github.com/syndtr/goleveldb v1.0.1-0.20190625010220-02440ea7a285
	github.com/tikv/pd v1.1.0-beta.0.20200923060638-9ef2f15063d6
	github.com/unrolled/render v0.0.0-20180914162206-b9786414de4d
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/zap v1.16.0
	golang.org/x/net v0.0.0-20200904194848-62affa334b73
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/sys v0.0.0-20200922070232-aee5d888a860
	golang.org/x/tools v0.0.0-20200923053713-ba800b16d873 // indirect
	google.golang.org/grpc v1.26.0
	gopkg.in/yaml.v2 v2.3.0 // indirect
)

go 1.13
