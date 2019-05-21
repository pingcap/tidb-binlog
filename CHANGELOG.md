# TiDB-Binlog
All notable changes to this project are documented in this file.

## [2.1.10]
- Fix all issues reported by static linters of golangci-lint [#599](https://github.com/pingcap/tidb-binlog/pull/599)
- Drainer: Ignore empty string when getting compressor config [#598](https://github.com/pingcap/tidb-binlog/pull/598)
- Update [parser](https://github.com/pingcap/parser) to the same [version](https://github.com/pingcap/parser/tree/361d2d4f774d779e3292d48af37bdce8f4ca88bf) as used in TiDB  [#608](https://github.com/pingcap/tidb-binlog/pull/608)
- Refine some log in storage module [#607](https://github.com/pingcap/tidb-binlog/pull/607)

## [2.1.9]
- Drainer: Fix when pk is handle and value overflow int64 [#574](https://github.com/pingcap/tidb-binlog/pull/574)
- Drainer: Remove pb compress config and change downstream name 'pb' to 'file' [#575](https://github.com/pingcap/tidb-binlog/pull/575)
- Reparo: Fix wrongly set update value [#576](https://github.com/pingcap/tidb-binlog/pull/576)
	- This bug is introduced since 2.1.7.

## [2.1.8]
- Add sql-mode config for drainer [#513](https://github.com/pingcap/tidb-binlog/pull/513)
- Drainer: add config ignore-tables [#526](https://github.com/pingcap/tidb-binlog/pull/526)
- Pump: Add syn-log option [#529](https://github.com/pingcap/tidb-binlog/pull/529)
- Add support for enabling gzip grpc compression for grpc [#530](https://github.com/pingcap/tidb-binlog/pull/530)
- Drainer: Ignore generated columns when updating [#531](https://github.com/pingcap/tidb-binlog/pull/531)

## [2.1.7]
- Use sync-diff-inspector check data and add pump restart test [#489](https://github.com/pingcap/tidb-binlog/pull/489)
- Support generated columns [#491](https://github.com/pingcap/tidb-binlog/pull/491)
- Cleanup useless code and outdated docs, Fix lint [#490](https://github.com/pingcap/tidb-binlog/pull/490)
- reparo: refactor some code[#489](https://github.com/pingcap/tidb-binlog/pull/498)
	- Skip scanning whole relative file for recovery.
	- Improve code and test, write to downstream concurrently by using `pkg/loader` 

## [2.1.6]
- Nothing's changed

## [2.1.5]
- Update the DDL binlog replication plan to guarantee the correctness of DDL [#466](https://github.com/pingcap/tidb-binlog/pull/466)
- Switch juju/errors to pingcap/error [#464](https://github.com/pingcap/tidb-binlog/pull/464)
- Open go mod and update despondencies [#475](https://github.com/pingcap/tidb-binlog/pull/475)
- Add package pkg/loader [#471](https://github.com/pingcap/tidb-binlog/pull/471)
- Add tool Arbiter sync from Kafka to Mysql [#441](https://github.com/pingcap/tidb-binlog/pull/441)

## [2.1.4]
- Nothing's changed

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
