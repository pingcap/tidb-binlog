# TiDB-Binlog 监控

这部分主要对 TiDB-Binlog 的状态、性能做监控，通过 Prometheus+Grafana 展现 metrics 数据，在下面一节会介绍如何搭建监控系统。

## 使用 Prometheus+Grafana
### 搭建监控系统
Prometheus Push Gateway
参考：https://github.com/prometheus/pushgateway

Prometheus Server
参考： https://github.com/prometheus/prometheus#install

Grafana
参考：http://docs.grafana.org


### 配置
#### pump/cistern/drainer 配置
设置 --metrics-addr 和 --metrics-interval 两个参数，其中 metrics-addr 设为 Push Gateway 的地址，metrics-interval 为 push 的频率，单位为秒，默认值为15

#### PushServer 配置
一般无需特殊配置，使用默认端口 9091 即可
 
+ Prometheus 配置
在 yaml 配置文件中添加 Push Gateway  地址：
```yaml
scrape_configs:
  # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
  - job_name: 'cistern'

    # Override the global default and scrape targets from this job every 5 seconds.
    scrape_interval: 5s
    
    honor_labels:true
    
    static_configs:
      - targets: ['host:port'] # 这里填写 pushgateway 地址
        labels:
                group: 'production'
```
#### Grafana 配置

+ 进入 Grafana Web 界面（默认地址: http://localhost:3000，默认账号: admin 密码: admin）

  点击 Grafana Logo -> 点击 Data Sources -> 点击 Add data source -> 填写 data source 信息 ( 注: Type 选 Prometheus，Url 为 Prometheus 地址，其他根据实际情况填写 ）

+ 导入 dashboard 配置文件

  点击 Grafana Logo -> 点击 Data Sources -> 点击 Import -> 选择需要的 dashboard [配置文件][1]上传 -> 选择对应的 data source


  [1]: https://github.com/pingcap/docs/tree/master/etc