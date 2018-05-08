package pump

import (
	"time"

	"github.com/ngaut/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"golang.org/x/net/context"
)

var (
	rpcCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "rpc_counter",
			Help:      "RPC counter for every rpc related operations.",
		}, []string{"method", "label"})

	rpcHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "rpc_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of rpc queries.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"method", "label"})

	writeBinlogSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "write_binlog_size",
			Help:      "write binlog size",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"label"})

	lossBinlogCacheCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "binlog_count",
			Help:      "Total loss binlog count",
		}, []string{"nodeID"})

	writeBinlogHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "write_binlog_duration_time",
			Help:      "Bucketed histogram of write time (s) of a binlog.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"label"})

	writeBinlogCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "write_binlog_error_count",
			Help:      "write binlog error count",
		}, []string{"label", "return"})

	readBinlogHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "read_binlog_duration_time",
			Help:      "Bucketed histogram of read time (s) of a binlog.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"label"})

	readBinlogCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "read_binlog_error_count",
			Help:      "read binlog error count",
		}, []string{"label", "return"})
	checkpointGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "checkpoint",
			Help:      "check position of local file",
		}, []string{"label"})
)

func init() {
	prometheus.MustRegister(rpcCounter)
	prometheus.MustRegister(rpcHistogram)
	prometheus.MustRegister(writeBinlogSizeHistogram)
	prometheus.MustRegister(readBinlogHistogram)
	prometheus.MustRegister(lossBinlogCacheCounter)
	prometheus.MustRegister(writeBinlogHistogram)
	prometheus.MustRegister(readBinlogCounter)
	prometheus.MustRegister(writeBinlogCounter)
	prometheus.MustRegister(checkpointGauge)
}

type metricClient struct {
	addr     string
	interval int
}

// Start run a loop of pushing metrics to Prometheus Pushgateway.
func (mc *metricClient) Start(ctx context.Context, pumpID string) {
	log.Debugf("start prometheus metrics client, addr=%s, internal=%ds", mc.addr, mc.interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(mc.interval) * time.Second):
			err := push.AddFromGatherer(
				"binlog",
				map[string]string{"instance": pumpID},
				mc.addr,
				prometheus.DefaultGatherer,
			)
			if err != nil {
				log.Errorf("could not push metrics to Prometheus Pushgateway: %v", err)
			}
		}
	}
}
