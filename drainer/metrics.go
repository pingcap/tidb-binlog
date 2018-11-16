package drainer

import (
	"os"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/drainer/executor"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"golang.org/x/net/context"
)

var (
	pumpPositionGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "pump_position",
			Help:      "position for each pump.",
		}, []string{"nodeID"})

	ddlJobsCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "ddl_jobs_total",
			Help:      "Total ddl jobs count been stored.",
		})

	errorCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "error_count",
			Help:      "Total count of error type",
		}, []string{"type"})

	disorderBinlogCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "disorder_binlog_count",
			Help:      "Total count of binlog which is disorder.",
		})

	eventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "event",
			Help:      "the count of sql event(dml, ddl).",
		}, []string{"type"})

	checkpointTSOGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "checkpoint_tso",
			Help:      "save checkpoint tso of drainer.",
		})

	executeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "execute_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a execute to sync data to downstream.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		})

	queryHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "query_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a query to sync data to downstream.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		}, []string{"type"})

	binlogReachDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "binlog_reach_duration_time",
			Help:      "Bucketed histogram of how long the binlog take to reach drainer since it's committed",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		}, []string{"nodeID"})

	readBinlogSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "read_binlog_size",
			Help:      "Bucketed histogram of size of a binlog.",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 20),
		}, []string{"nodeID"})
)

var registry = prometheus.NewRegistry()

func init() {
	registry.MustRegister(prometheus.NewProcessCollector(os.Getpid(), ""))
	registry.MustRegister(prometheus.NewGoCollector())
	registry.MustRegister(pumpPositionGauge)
	registry.MustRegister(ddlJobsCounter)
	registry.MustRegister(errorCount)
	registry.MustRegister(checkpointTSOGauge)
	registry.MustRegister(eventCounter)
	registry.MustRegister(executeHistogram)
	registry.MustRegister(binlogReachDurationHistogram)
	registry.MustRegister(readBinlogSizeHistogram)
	registry.MustRegister(queryHistogramVec)
	executor.QueryHistogramVec = queryHistogramVec
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
				registry,
			)
			if err != nil {
				log.Errorf("could not push metrics to Prometheus Pushgateway: %v", err)
			}
		}
	}
}
