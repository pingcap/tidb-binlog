# TiDB 集群备份和恢复操作手册

## 概述
tools/checkpoint.sh 利用 TiDB-Binlog + mydumper 实现全量备份 TiDB 集群某个时间点数据。

tools/recovery.sh 利用 TiDB-Binlog + myloader 实现数据的恢复， 并且支持同步模式。
* 恢复模式：将数据全量迁移到其他集群。如集群故障迁移数据的场景
* 同步模式：恢复数据后进行数据的实时同步。 如添加从库的场景

## 前置准备
* 安装
```shell
# 下载 TiDB-Binlog 压缩包
wget http://download.pingcap.org/binlog-latest-linux-amd64.tar.gz

# 解开压缩包
tar -xzf binlog-latest-linux-amd64.tar.gz

# 下载 mytools 压缩包
wget http://download.pingcap.org/mydumper-linux-amd64.tar.gz

# 解开压缩包到
tar -xzf mydumper-linux-amd64.tar.gz
mv mydumper-linux-amd64/bin/* binlog-latest-linux-amd64/bin/
```
* 部署 TiDB-Binlog，[参考文档][1]（只需要部署 pump 和 cistern）

## 数据备份
 1. 选择备份时间，在没有 DDL 操作的时间点进行备份
 2. 运行 ./tools/checkpoint.sh 进行备份, 命令行参数如下：
 
| 参数名         |  说明     |
| --------       | -----:    |
| -h, --host     | TiDB 的 HOST   |
| -p, --port     | TiDB 的 PORT   |
| -u, --user     | TiDB 的用户名  |
| -p, --password | TiDB 的密码    |
| -c, --cistern-addr| TiDB-Binlog 的 cistern 接口地址|
| -o, --outputdir| dump files 的输出目录 |
| -F, --chunk-filesize|  把 table 分割成指定大小文件分别储存，单位为 MB (建议大小 64)|

## 数据恢复
 1. 启动 TIDB 集群/mysql, 注意必须为全新的集群，或者清空集群全部数据。
 2. 配置 drainer 启动参数, 在配置文件(./conf/drainer.toml)中修改
  
| 参数名         |  说明     |
| --------       | -----:    |
| to:host     | 目标数据库的 HOST   |
| to:port     | 目标数据库的 PORT   |
| to:user     | 目标数据库的用户名  |
| to:password | 目标数据库的密码    |
| client:host | cistern 的 HOST     |
| client:port | cistern 的 PORT     |
| data-dir    | drainer 数据存放目录|
| ignore-schemas| 需要过滤掉的 databases|
| log-file    | drainer log 存放目录 |
| metrics-addr| prometheus 接口地址|
 3. 运行 ./tools/recovery.sh 进行集群恢复/同步, 命令行参数如下：
 
| 参数名         |  说明     |
| --------       | -----:    |
| -h, --host     | TiDB 的 HOST   |
| -p, --port     | TiDB 的 PORT   |
| -u, --user     | TiDB 的用户名  |
| -p, --password | TiDB 的密码    |
| -c, --cistern-addr| TiDB-Binlog 的 cistern 接口地址|
| -d, --directory| 指定 dump files 的存放目录 |
| -t, --threads|  load dump files 的进行成个数|
| -r, --is-recovery| 是否开启恢复模式，不开启进入同步模式|


  [1]: https://github.com/pingcap/tidb-binlog/blob/master/docs/doc-cn.md