# TiDB-Binlog 部署文档

## 目录

+ TiDB-Binlog 简介 & 整体架构
  - [TiDB-Binlog 功能特性](#tidb-binlog-简介)
  - [TiDB-Binlog 整体架构](#tidb-binlog-架构)
+ 安装 & 部署
  - [安装](#tidb-binlog-安装)
  - [部署建议](#tidb-binlog-部署建议)
+ 运维 & 监控
 - [整体监控框架概述](https://github.com/pingcap/docs-cn/blob/master/op-guide/monitor-overview.md)
 - [组件状态 API & 监控](./monitor.md)

## TiDB-Binlog 简介

TiDB-Binlog 用于收集 TiDB 的 Binlog，并提供实时备份和同步功能的商业工具。

TiDB-Binlog 支持以下功能场景:

* *数据同步*:       同步 TiDB 集群数据到其他数据库
* *实时备份和恢复*:  备份 TiDB 集群数据，同时可以用于 TiDB 集群故障时恢复

## TiDB-Binlog 架构

首先介绍 TiDB-Binlog 的整体架构。

![TiDB-Binlog 架构](./architecture.jpeg)

TiDB-Binlog 集群主要分为两个组件：

### Pump 

Pump 是一个守护进程，在每个 TiDB 的主机上后台运行。他的主要功能是实时记录 TiDB 产生的 Binlog 并顺序写入磁盘文件

### Drainer

Drainer 从各个 Pump 节点收集 Binlog，并按照在 TiDB 中事务的提交顺序转化为指定数据库兼容的 SQL 语句，最后同步到目的数据库或者写到顺序文件

## TiDB-Binlog 安装

### 下载官方 Binary

- (CentOS 7+, Ubuntu 14.04+)

```bash
# 下载压缩包
wget http://download.pingcap.org/tidb-binlog-latest-linux-amd64.tar.gz
wget http://download.pingcap.org/tidb-binlog-latest-linux-amd64.sha256

# 检查文件完整性，返回 ok 则正确
sha256sum -c tidb-binlog-latest-linux-amd64.sha256

# 解开压缩包
tar -xzf tidb-binlog-latest-linux-amd64.tar.gz
cd tidb-binlog-latest-linux-amd64
```

## TiDB-Binlog 部署建议

*   搭建全新的 TiDB Cluster，推荐部署顺序 pd-server -> tikv-server -> pump -> tidb-server -> drainer

*   对已有的 TiDB Cluster 部署 binlog，需要对每台 tidb 实例滚动进行以下操作
    * stop tidb-server
    * 部署 pump
    * 改动 tidb-server 的 binlog-socket，然后重启 tidb-server

### 注意

*   需要为一个 TiDB 集群中的每台 TiDB server 部署一个 pump，目前 TiDB server 只支持以 unix socket 方式的输出 binlog。

    我们设置 TiDB 启动参数 binlog-socket 为对应的 pump 的参数 socket 所指定的 unix socket 文件路径，最终部署结构如下图所示：

    ![TiDB pump 模块部署结构](./tidb_pump_deployment.jpeg)

*   drainer 不支持对 ignore schemas（在过滤列表中的 schemas） 的 table 进行 rename DDL 操作

*   在已有的 TiDB 集群中启动 drainer，一般需要全量备份 并且获取 savepoint，然后导入全量备份，最后启动 drainer 从 savepoint 开始同步；

    为了保证数据的完整性，在 pump 运行 10 分钟左右后按顺序进行下面的操作

    *  以 gen-savepoint model 运行 drainer 生成 drainer savepint 文件，`bin/drainer -gen-savepoint --data-dir= ${drainer_savepoint_dir} --pd-urls=${pd_urls}`
    *  全量备份，例如 mydumper 备份 tidb
    *  全量导入备份到目标系统
    *  设置 savepoint 文件路径，然后启动 drainer， `bin/drainer --config=conf/drainer.toml --data-dir=${drainer_savepoint_dir}`

*   drainer 输出的 pb, 需要在配置文件设置下面的参数
    ```
    [syncer]
    db-type = "pb"
    disable-dispatch = true

    [syncer.to]
    dir = "/path/pb-dir"
    ```


### 示例及参数解释

1.  Pump

    示例

    ```bash
    ./bin/pump -config pump.toml
    ```
    
    参数解释

    ```
    Usage of pump:
    -node-id string
        pump 的 node ID (如果不指定则为 hostname:port)
    -addr string
        pump 提供服务的 rpc 地址(默认 "127.0.0.1:8250")
    -advertise-addr string
        pump 对外提供服务的 rpc 地址(默认 "127.0.0.1:8250")
    -config string
        配置文件路径,如果你指定了配置文件，pump 会首先读取配置文件的配置
        如果对应的配置在命令行参数里面也存在，pump 就会使用命令行参数的配置来覆盖配置文件里面的
    -data-dir string
        pump 数据存储位置路径
    -L string
        日志输出信息等级设置: debug, info, warn, error, fatal (默认 "info")
    -gc int
        日志最大保留天数 (默认 7)， 设置为 0 可永久保存
    -heartbeat-interval uint
        pump 向 pd 发送心跳间隔 (单位 秒)
    -log-file string
        log 文件路径
    -log-rotate string
        log 文件切换频率, hour/day
    -pd-urls string
        pd 集群节点的地址 (默认 "http://127.0.0.1:2379")
    -socket string
        unix socket 模式服务监听地址 (默认 unix:///tmp/pump.sock)
    -metrics-addr string
       prometheus pushgataway 地址，不设置则禁止上报监控信息
    -metrics-interval int
       监控信息上报频率 (默认 15，单位 秒)
    -V
        打印版本信息
    ```

    配置文件
    ```
    # pump Configuration.

    # pump 提供服务的 rpc 地址(默认 "127.0.0.1:8250")
    addr = "127.0.0.1:8250"

    # binlog 最大保留天数 (默认 7)， 设置为 0 可永久保存
    gc = 7

    #  pump 数据存储位置路径
    data-dir = "data.pump"

    # pump 向 pd 发送心跳间隔 (单位 秒)
    heartbeat-interval = 3

    # pd 集群节点的地址 (默认 "http://127.0.0.1:2379")
    pd-urls = "http://127.0.0.1:2379"

    # unix socket 模式服务监听地址 (默认 unix:///tmp/pump.sock)
    socket = "unix:///tmp/pump.sock"
    ```


2.  Drainer

    示例

    ```bash
    ./bin/drainer -config drainer.toml
    ```
    
    参数解释

    ```
    Usage of drainer:
    -L string
        日志输出信息等级设置: debug, info, warn, error, fatal (默认 "info")
    -addr string
        drainer 提供服务的地址(默认 "127.0.0.1:8249")
    -c int
        同步下游的并发数，该值设置越高同步的吞吐性能越好 (default 1)
    -config string
       配置文件路径, drainer 会首先读取配置文件的配置
       如果对应的配置在命令行参数里面也存在，drainer 就会使用命令行参数的配置来覆盖配置文件里面的
    -data-dir string
       drainer 数据存储位置路径 (默认 "data.drainer")
    -dest-db-type string
        drainer 下游服务类型 (默认为 mysql)
    -detect-interval int
        向 pd 查询在线 pump 的时间间隔 (默认 10，单位 秒)
    -disable-dispatch
        是否禁用拆分单个 binlog 的 sqls 的功能，如果设置为 true，则按照每个 binlog 顺序依次还原成单个事务进行同步
    -gen-savepoint
        如果设置为 true, 则只生成 drainer 的 savepoint meta 文件, 可以配合 mydumper 使用
    -ignore-schemas string
        db 过滤列表 (默认 "INFORMATION_SCHEMA,PERFORMANCE_SCHEMA,mysql,test"),   
        不支持对 ignore schemas 的 table 进行 rename DDL 操作
    -log-file string
        log 文件路径
    -log-rotate string
        log 文件切换频率, hour/day
    -metrics-addr string
       prometheus pushgataway 地址，不设置则禁止上报监控信息
    -metrics-interval int
       监控信息上报频率 (默认 15，单位 秒)
    -pd-urls string
       pd 集群节点的地址 (默认 "http://127.0.0.1:2379")
    -txn-batch int
       输出到下游数据库一个事务的 sql 数量 (default 1)
    ```

    配置文件
    ```
    # drainer Configuration.

    # drainer 提供服务的地址(默认 "127.0.0.1:8249")
    addr = "127.0.0.1:8249"

    # 向 pd 查询在线 pump 的时间间隔 (默认 10，单位 秒)
    detect-interval = 10

    # drainer 数据存储位置路径 (默认 "data.drainer")
    data-dir = "data.drainer"

    # pd 集群节点的地址 (默认 "http://127.0.0.1:2379")
    pd-urls = "http://127.0.0.1:2379"

    # syncer Configuration.
    [syncer]

    # db 过滤列表 (默认 "INFORMATION_SCHEMA,PERFORMANCE_SCHEMA,mysql,test"),   
    # 不支持对 ignore schemas 的 table 进行 rename DDL 操作
    ignore-schemas = "INFORMATION_SCHEMA,PERFORMANCE_SCHEMA,mysql"

    # 输出到下游数据库一个事务的 sql 数量 (default 1)
    txn-batch = 1

    # 同步下游的并发数，该值设置越高同步的吞吐性能越好 (default 1)
    worker-count = 1

    disable-dispatch = true

    # drainer 下游服务类型 (默认为 mysql)
    # valid values are "mysql", "pb"
    db-type = "mysql"

    ##replicate-do-db priority over replicate-do-table if have same db name
    ##and we support regex expression , start with '~' declare use regex expression.
    #
    #replicate-do-db = ["~^b.*","s1"]
    #[[replicate-do-table]]
    #db-name ="test"
    #tbl-name = "log"

    #[[replicate-do-table]]
    #db-name ="test"
    #tbl-name = "~^a.*"

    # the downstream mysql protocol database
    [syncer.to]
    host = "127.0.0.1"
    user = "root"
    password = ""
    port = 3306

    # uncomment this if you want to use pb as db-type
    # [syncer.to]
    # dir = "data.drainer"
    ```
