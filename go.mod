module github.com/pingcap/tidb-binlog

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.3.0
	github.com/Shopify/sarama v1.18.0
	github.com/Shopify/toxiproxy v2.1.3+incompatible // indirect
	github.com/beorn7/perks v0.0.0-20160229213445-3ac7bf7a47d1 // indirect
	github.com/cloudfoundry-incubator/candiedyaml v0.0.0-20160429080125-99c3df83b515 // indirect
	github.com/cockroachdb/cmux v0.0.0-20160228191917-112f0506e774 // indirect
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/coreos/bbolt v1.3.1-coreos.5 // indirect
	github.com/coreos/etcd v3.2.14+incompatible
	github.com/coreos/go-semver v0.0.0-20150304020126-568e959cd898 // indirect
	github.com/coreos/go-systemd v0.0.0-20160815185329-bfdc81d0d7e0 // indirect
	github.com/coreos/pkg v0.0.0-20160620232715-fa29b1d70f0b // indirect
	github.com/cznic/golex v0.0.0-20160422121650-da5a7153a510 // indirect
	github.com/cznic/mathutil v0.0.0-20160613104831-78ad7f262603 // indirect
	github.com/cznic/parser v0.0.0-20160622100904-31edd927e5b1 // indirect
	github.com/cznic/sortutil v0.0.0-20150617083342-4c7342852e65 // indirect
	github.com/cznic/strutil v0.0.0-20150430124730-1eb03e3cc9d3 // indirect
	github.com/cznic/y v0.0.0-20160420101755-9fdf92d4aac0 // indirect
	github.com/davecgh/go-spew v0.0.0-20180830191138-d8f796af33cc // indirect
	github.com/dgrijalva/jwt-go v3.0.0+incompatible // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/eapache/go-resiliency v1.1.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v0.0.0-20180227141424-093482f3f8ce // indirect
	github.com/eknkc/amber v0.0.0-20171010120322-cdade1c07385 // indirect
	github.com/etcd-io/gofail v0.0.0-20180808172546-51ce9a71510a // indirect
	github.com/fsnotify/fsnotify v1.4.7 // indirect
	github.com/ghodss/yaml v0.0.0-20160604002925-aa0c86205766 // indirect
	github.com/go-sql-driver/mysql v1.4.0
	github.com/gogo/protobuf v0.0.0-20171007142547-342cbe0a0415
	github.com/golang-collections/collections v0.0.0-20130729185459-604e922904d3 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/groupcache v0.0.0-20160516000752-02826c3e7903 // indirect
	github.com/golang/lint v0.0.0-20181011164241-5906bd5c48cd // indirect
	github.com/golang/protobuf v1.1.0
	github.com/golang/snappy v0.0.0-20150730031844-723cc1e459b8 // indirect
	github.com/google/btree v0.0.0-20161217183710-316fb6d3f031 // indirect
	github.com/google/gofuzz v0.0.0-20170612174753-24818f796faf
	github.com/gorilla/context v1.1.1 // indirect
	github.com/gorilla/mux v1.6.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v0.0.0-20180820150422-93bf4626fba7 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.3.1 // indirect
	github.com/hpcloud/tail v1.0.0 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/juju/errors v0.0.0-20160809030848-6f54ff631840
	github.com/juju/loggo v0.0.0-20180524022052-584905176618 // indirect
	github.com/juju/testing v0.0.0-20180920084828-472a3e8b2073 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/montanaflynn/stats v0.0.0-20180911141734-db72e6cae808 // indirect
	github.com/ngaut/deadline v0.0.0-20150302045450-fae8f9dfd704 // indirect
	github.com/ngaut/log v0.0.0-20160810023011-cec23d3e10b0
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7 // indirect
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef // indirect
	github.com/onsi/ginkgo v1.6.0 // indirect
	github.com/onsi/gomega v1.4.0
	github.com/opentracing/opentracing-go v0.0.0-20180606204148-bd9c31933947 // indirect
	github.com/petar/GoLLRB v0.0.0-20130427215148-53be0d36a84c // indirect
	github.com/pierrec/lz4 v2.0.5+incompatible // indirect
	github.com/pingcap/check v0.0.0-20171206051426-1c287c953996
	github.com/pingcap/errors v0.11.0
	github.com/pingcap/goleveldb v0.0.0-20161010101021-158edde5a354 // indirect
	github.com/pingcap/kvproto v0.0.0-20181010074705-0ba3ca8a6e37 // indirect
	github.com/pingcap/parser v0.0.0-20181210061630-27e9d3e251d4 // indirect
	github.com/pingcap/pd v2.0.5+incompatible
	github.com/pingcap/tidb v2.1.0-beta.0.20180823032518-ef6590e1899a+incompatible
	github.com/pingcap/tidb-tools v2.1.3-0.20190215110732-23405d82dbe6+incompatible
	github.com/pingcap/tipb v0.0.0-20180711115030-4141907f6909
	github.com/pkg/errors v0.8.0
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v0.8.0
	github.com/prometheus/client_model v0.0.0-20150212101744-fa8ad6fec335 // indirect
	github.com/prometheus/common v0.0.0-20160623151427-4402f4e5ea79 // indirect
	github.com/prometheus/procfs v0.0.0-20160411190841-abf152e5f3e9 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20180503174638-e2704e165165
	github.com/samuel/go-zookeeper v0.0.0-20170815201139-e6b59f6144be
	github.com/siddontang/go v0.0.0-20161005110831-1e9ce2a5ac40
	github.com/sirupsen/logrus v0.0.0-20180830201151-78fa2915c1fa
	github.com/soheilhy/cmux v0.1.2
	github.com/spaolacci/murmur3 v0.0.0-20180118202830-f09979ecbc72 // indirect
	github.com/stretchr/testify v1.2.2 // indirect
	github.com/syndtr/goleveldb v0.0.0-20180708030551-c4c61651e9e3
	github.com/twinj/uuid v0.0.0-20150629100731-70cac2bcd273 // indirect
	github.com/uber-go/atomic v1.3.2 // indirect
	github.com/uber/jaeger-client-go v0.0.0-20180903021652-12b4b58a7d81 // indirect
	github.com/uber/jaeger-lib v0.0.0-20180112221534-34d9cc24e47a // indirect
	github.com/ugorji/go v0.0.0-20151028022000-f1f1a805ed36 // indirect
	github.com/unrolled/render v0.0.0-20180807193321-4206df6ff701
	github.com/urfave/negroni v1.0.0 // indirect
	github.com/xiang90/probing v0.0.0-20160813154853-07dd2e8dfe18 // indirect
	github.com/zanmato1984/clickhouse v0.0.0-20180829183406-6c32f7b4cd79
	go.uber.org/atomic v1.3.2 // indirect
	golang.org/x/crypto v0.0.0-20150218234220-1351f936d976 // indirect
	golang.org/x/lint v0.0.0-20181011164241-5906bd5c48cd // indirect
	golang.org/x/net v0.0.0-20180724234803-3673e40ba225
	golang.org/x/sync v0.0.0-20180314180146-1d60e4601c6f
	golang.org/x/sys v0.0.0-20161006025142-8d1157a43547
	golang.org/x/time v0.0.0-20170420181420-c06e80d9300e // indirect
	golang.org/x/tools v0.0.0-20181012201414-c0eb142035b5 // indirect
	google.golang.org/appengine v0.0.0-20180731164958-4216e58b9158 // indirect
	google.golang.org/genproto v0.0.0-20181004005441-af9cb2a35e7f // indirect
	google.golang.org/grpc v1.12.0
	gopkg.in/airbrake/gobrake.v2 v2.0.9 // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/fsnotify.v1 v1.4.7 // indirect
	gopkg.in/gemnasium/logrus-airbrake-hook.v2 v2.1.2 // indirect
	gopkg.in/mgo.v2 v2.0.0-20180705113604-9856a29383ce // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0-20170531160350-a96e63847dc3
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v2 v2.0.0-20170407172122-cd8b52f8269e // indirect
)
