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

Drainer 从各个 Pump 节点收集 Binlog，并按照在 TiDB 中事务的提交顺序转化为指定数据库兼容的 SQL 语句，最后同步到目的数据库或者文件系统

## TiDB-Binlog 安装

### 从源码编译

```
git clone https://github.com/pingcap/tidb-binlog.git  # git 下载源代码
make build   # 编译所有组件
```

也可以分开编译

```
make pump    # 编译 pump
make drainer # 编译 drainer
```

### 下载官方 Binary

- (CentOS 7+, Ubuntu 14.04+)

```bash
# 下载压缩包
wget http://download.pingcap.org/binlog-latest-linux-amd64.tar.gz

# 检查文件完整性，返回 ok 则正确
sha256sum -c binlog-latest-linux-amd64.sha256

# 解开压缩包
tar -xzf binlog-latest-linux-amd64.tar.gz
cd binlog-latest-linux-amd64
```

## TiDB-Binlog 部署建议

TiDB-Binlog 推荐部署启动顺序  PD -> TiKV -> Pump -> TiDB -> Drainer

### 注意

*   需要为一个 TiDB 集群中的每台 TiDB server 部署一个 pump，目前 TiDB server 只支持以 unix socket 方式的输出 binlog。

    我们设置 TiDB 启动参数 binlog-socket 为对应的 pump 的参数 socket 所指定的 unix socket 文件路径，最终部署结构如下图所示：

    ![TiDB pump 模块部署结构](./tidb_pump_deployment.jpeg)

*   drainer 不支持对 ignore schemas（在过滤列表中的 schemas） 的 table 进行 rename DDL 操作

### 示例及参数解释

1.  Pump

    示例

    ```bash
    ./bin/pump -socket unix:///tmp/pump.sock \
               -pd-urls http://127.0.0.1:2379 \
               -data-dir ./data.pump
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
    -version
        打印版本信息
    ```

2.  Drainer

    示例

    ```bash
    ./bin/drainer -dest-db-type mysql \
                  -ignore-schemas INFORMATION_SCHEMA,PERFORMANCE_SCHEMA,mysql \
                  -data-dir ./data.drainer 
    ```
    
    参数解释

    ```
    Usage of drainer:
    -L string
        日志输出信息等级设置: debug, info, warn, error, fatal (默认 "info")
    -addr string
        drainer 提供服务的 rpc 地址(默认 "127.0.0.1:8249")
    -c int
        并发输出到下游数据库或者文件系统 goroutine 数量 (default 1)
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
        是否禁用拆分 TiDB binlog 事务中 sql 的功能，如果设置为 true，则按照每个 binlog顺序依次还原成单个事务进行同步
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
