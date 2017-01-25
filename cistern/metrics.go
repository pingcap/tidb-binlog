package cistern

import (
	"time"

	"github.com/ngaut/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"golang.org/x/net/context"
)

var (
	windowGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "cistern",
			Name:      "window",
			Help:      "DepositWindow boundary.",
		}, []string{"marker"})

	savepointGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "cistern",
			Name:      "savepoint",
			Help:      "Save point for each node.",
		}, []string{"nodeID"})

	rpcCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "cistern",
			Name:      "rpc_counter",
			Help:      "RPC counter for every rpc related operations.",
		}, []string{"method", "label"})

	rpcHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "cistern",
			Name:      "rpc_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of rpc queries.",
			Buckets:   prometheus.ExponentialBuckets(0.25, 2, 13),
		}, []string{"method", "label"})

	binlogCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "cistern",
			Name:      "binlog_count_total",
			Help:      "Total binlog count been stored.",
		})

	ddlJobsCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "cistern",
			Name:      "ddl_jobs_total",
			Help:      "Total ddl jobs count been stored.",
		})

	tikvQueryCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "cistern",
			Name:      "query_tikv_count",
			Help:      "Total count that queried tikv.",
		})

	errorBinlogCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "cistern",
			Name:      "error_binlog_count",
			Help:      "Total count of binlog that store too late.",
		})
)

func init() {
	prometheus.MustRegister(windowGauge)
	prometheus.MustRegister(savepointGauge)
	prometheus.MustRegister(rpcCounter)
	prometheus.MustRegister(rpcHistogram)
	prometheus.MustRegister(binlogCounter)
	prometheus.MustRegister(ddlJobsCounter)
	prometheus.MustRegister(tikvQueryCount)
	prometheus.MustRegister(errorBinlogCount)
}

type metricClient struct {
	addr     string
	interval int
}

// Start run a loop of pushing metrics to Prometheus Pushgateway.
func (mc *metricClient) Start(ctx context.Context) {
	log.Debugf("start prometheus metrics client, addr=%s, internal=%ds", mc.addr, mc.interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(mc.interval) * time.Second):
			err := push.AddFromGatherer(
				"binlog",
				push.HostnameGroupingKey(),
				mc.addr,
				prometheus.DefaultGatherer,
			)
			if err != nil {
				log.Errorf("could not push metrics to Prometheus Pushgateway: %v", err)
			}
		}
	}
}
