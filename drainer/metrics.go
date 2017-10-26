package drainer

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
			Subsystem: "drainer",
			Name:      "window",
			Help:      "DepositWindow boundary.",
		}, []string{"marker"})

	savepointGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "savepoint",
			Help:      "Save point for each node.",
		}, []string{"nodeID"})

	rpcCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "rpc_counter",
			Help:      "RPC counter for every rpc related operations.",
		}, []string{"method", "label"})

	rpcHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "rpc_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of rpc queries.",
			Buckets:   prometheus.ExponentialBuckets(0.25, 2, 13),
		}, []string{"method", "label"})

	binlogCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "binlog_count_total",
			Help:      "Total binlog count been stored.",
		})

	ddlJobsCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "ddl_jobs_total",
			Help:      "Total ddl jobs count been stored.",
		})

	tikvQueryCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "query_tikv_count",
			Help:      "Total count that queried tikv.",
		})

	errorBinlogCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "error_binlog_count",
			Help:      "Total count of binlog that store too late.",
		})
	eventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "event",
			Help:      "the count of sql event(dml, ddl).",
		}, []string{"type"})
	positionGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "position",
			Help:      "save position of drainer.",
		})
	txnHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "txn_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
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
	prometheus.MustRegister(positionGauge)
	prometheus.MustRegister(eventCounter)
	prometheus.MustRegister(txnHistogram)
}

type metricClient struct {
	addr     string
	interval int
}

// Start run a loop of pushing metrics to Prometheus Pushgateway.
func (mc *metricClient) Start(ctx context.Context, drainerID string) {
	log.Debugf("start prometheus metrics client, addr=%s, internal=%ds", mc.addr, mc.interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(mc.interval) * time.Second):
			err := push.AddFromGatherer(
				"binlog",
				map[string]string{"instance": drainerID},
				mc.addr,
				prometheus.DefaultGatherer,
			)
			if err != nil {
				log.Errorf("could not push metrics to Prometheus Pushgateway: %v", err)
			}
		}
	}
}
