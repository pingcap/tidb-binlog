# TiDB-Binlog
All notable changes to this project are documented in this file.

## [2.1.3]
+ Pump
	- Add a API to get binlog by ts [#449](https://github.com/pingcap/tidb-binlog/pull/449)

+ Drainer
	- Fix history job not sorted by schema version [#444](https://github.com/pingcap/tidb-binlog/pull/444)
	- Change default config value to more reasonable value [#439](https://github.com/pingcap/tidb-binlog/pull/439) [#442](https://github.com/pingcap/tidb-binlog/pull/442)
	- Fix not skip rollback state ddl job [#432](https://github.com/pingcap/tidb-binlog/pull/432)
	- Fix data may not consistent when no pk but has uk [#421](https://github.com/pingcap/tidb-binlog/pull/421)

+ Other
	- Add integration test for node status [#416](https://github.com/pingcap/tidb-binlog/pull/416)

## [2.1.4]
- Nothing's changed

## [2.1.5]
- Update the DDL binlog replication plan to guarantee the correctness of DDL [#466](https://github.com/pingcap/tidb-binlog/pull/466)
- Switch juju/errors to pingcap/error [#464](https://github.com/pingcap/tidb-binlog/pull/464)
- Open go mod and update despondencies [#475](https://github.com/pingcap/tidb-binlog/pull/475)
- Add package pkg/loader [#471](https://github.com/pingcap/tidb-binlog/pull/471)
- Add tool Arbiter sync from Kafka to Mysql [#441](https://github.com/pingcap/tidb-binlog/pull/441)

## [2.1.6]
- Nothing's changed

## [2.1.7]
- Use sync-diff-inspector check data and add pump restart test [#489](https://github.com/pingcap/tidb-binlog/pull/489)
- Support generated columns [#491](https://github.com/pingcap/tidb-binlog/pull/491)
- Cleanup useless code and outdated docs, Fix lint[#490](https://github.com/pingcap/tidb-binlog/pull/490)
- reparo: refactor some code[#489](https://github.com/pingcap/tidb-binlog/pull/498)
	- Skip scanning whole relative file for recovery.
	- Improve code and test, write to downstream concurrently by using `pkg/loader` 

