# TiDB-Binlog
All notable changes to this project are documented in this file.

## [2.1.3]
+ Pump
	- Add a API to get binlog by ts [#449](https://github.com/pingcap/tidb-binlog/pull/449)

+ Drainer
	- Fix history not job sorted by schema version [#444](https://github.com/pingcap/tidb-binlog/pull/444)
	- Change more reasonable default config value [#439](https://github.com/pingcap/tidb-binlog/pull/439)[#442] (https://github.com/pingcap/tidb-binlog/pull/442)
	- Fix not skip rollback state ddl job [#432](https://github.com/pingcap/tidb-binlog/pull/432)
	- Fix data may not consistent when no pk but has uk [#421](https://github.com/pingcap/tidb-binlog/pull/421)

+ Other
	- Add integration test for node status [#416](https://github.com/pingcap/tidb-binlog/pull/416)

