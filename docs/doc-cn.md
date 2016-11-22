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
![architecture](./architecture.jpeg)
TiDB-Binlog 集群主要分为三个组件：
### Pump 
Pump 是一个守护进程，在每个 TiDB 的主机上后台运行。他的主要功能是实时记录 TiDB 产生的 Binlog 并顺序写入磁盘文件

### Cistern
Cistern 从各个 Pump 节点收集 Binlog，并按照在 TiDB 中事务的提交顺序进行持久化保存；并向 Drainer 提供从任意位置顺序读取 Binlog 的功能

### Drainer
Drainer 从 Cistern 顺序读取 Binlog，然后转化为指定数据库兼容的 SQL 语句，最后同步到目的数据库或者文件系统


## TiDB-Binlog 安装

### 从源码编译
```
git clone https://github.com/pingcap/tidb-binlog.git  # git 下载源代码

make build   # 编译所有组件
```

也可以分开编译
```
make pump    # 编译 pump

make cistern # 编译 cistern

make drainer # 编译 drainer
```

### 下载官方 Binary

#### Linux (CentOS 7+, Ubuntu 14.04+)

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

TiDB-Binlog 推荐部署启动顺序  PD -> TiKV -> Pump -> TiDB -> Cistern -> Drainer

## 启动参数

1. Pump.

    ```bash
    ./bin/pump -socket unix:///tmp/pump.sock \
               -pd-urls http://127.0.0.1:2379 \
               -data-dir data.pump
    ```
    
2. Cistern.

    ```bash
    ./bin/cistern -deposit-window-period 10 \
                  -pd-urls http://127.0.0.1:2379 \
                  -data-dir data.cistern
    ```

3. Drainer.

    ```bash
    ./bin/drainer -dest-db-type mysql \
                  -ignore-schemas INFORMATION_SCHEMA,PERFORMANCE_SCHEMA,mysql \
                  -data-dir data.drainer 
    ```
