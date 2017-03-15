# TiDB-Binlog 总览

TiDB-Binlog 是一个用于收集 TiDB 的 Binlog，并提供实时备份和同步功能的商业工具。

## TiDB-Binlog 架构

首先介绍 TiDB-Binlog 的整体架构。
![architecture](./binlog_architecture.jpeg)
TiDB-Binlog 集群主要分为三个组件：
### Pump 
Pump 是一个守护进程，用于接收来自 TiDB 的 Binlog，并以追加的方式写入磁盘文件。

### Cistern
Cistern 从 TiDB 分布式集群的各个 Pump 节点收集 Binlog，然后将 Binlog 按照在 TiDB 中提交的顺序进行持久化保存；并向 Drainer 提供从任意历史时间点顺序读取 Binlog 的功能。

### Drainer
Drainer 根据指定的时间点从 Cistern 顺序读取 Binlog，然后转化为指定格式的 SQL 语句，最后同步到目的数据库或者文件系统。



## 部署建议

TiDB-Binlog 推荐部署启动顺序 PD -> TiKV -> Pump -> TiDB -> Cistern -> drainer。<br>

## 启动参数

1. Pump.

    ```bash
    ./bin/pump -socket unix:///tmp/pump.sock \
               -pd-urls http://127.0.0.1:2379
    ```
    
2. Cistern.

    ```bash
    ./bin/cistern -cluster-id 1
    ```

3. Drainer.

    ```bash
    ./bin/drainer -dest-db-type mysql 
                  -ignore-schemas INFORMATION_SCHEMA,PERFORMANCE_SCHEMA,mysql \
                  -data-dir data.drainer 
    ```

一些说明：
* 对于非新 TiDB 集群，<br>
      * 首先部署 Pump， 接着修改启动参数重启 TiDB 实例，然后启动 Cistern <br>
      * 确认 Pump／Cisetrn 正常工作后，利用 mydumper／myloader 对 TiDB 集群进行全量 dump，然后倒入到目标数据库 <br>
      * 以 dump 时间点为初始化时间点参数来启动 drainer <br>
* 部署的时候，建议对每个 TiDB 实例部署一个 Pump
* Cistern 默认只保存 7 天的历史 Binlog
* Drainer 目前只支持 mysql 协议的转换和同步
