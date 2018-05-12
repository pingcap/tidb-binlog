package pump

import (
	"time"

	"github.com/ngaut/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	rpcHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "rpc_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of rpc queries.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		}, []string{"method", "label"})

	lossBinlogCacheCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "loss_binlog_count",
			Help:      "Total loss binlog count",
		})

	writeBinlogSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "write_binlog_size",
			Help:      "write binlog size",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 20),
		}, []string{"label"})

	writeBinlogHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "write_binlog_duration_time",
			Help:      "Bucketed histogram of write time (s) of a binlog.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		}, []string{"label"})

	writeErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "write_error_count",
			Help:      "write binlog error count",
		}, []string{"label"})

	readBinlogHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "read_binlog_duration_time",
			Help:      "Bucketed histogram of read time (s) of a binlog.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		}, []string{"label"})

	readErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "read_error_count",
			Help:      "read binlog error count",
		}, []string{"label"})

	corruptionBinlogCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "corruption_binlog_count",
			Help:      "corruption binlog count",
		})

	checkpointGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "checkpoint",
			Help:      "check position of local file",
		}, []string{"label"})
)

func init() {
	prometheus.MustRegister(rpcHistogram)
	prometheus.MustRegister(writeBinlogSizeHistogram)
	prometheus.MustRegister(readBinlogHistogram)
	prometheus.MustRegister(lossBinlogCacheCounter)
	prometheus.MustRegister(writeBinlogHistogram)
	prometheus.MustRegister(readErrorCounter)
	prometheus.MustRegister(writeErrorCounter)
	prometheus.MustRegister(checkpointGauge)
	prometheus.MustRegister(corruptionBinlogCounter)
}

type metricClient struct {
	addr     string
	interval int
}

// Start run a loop of pushing metrics to Prometheus Pushgateway.
func (mc *metricClient) Start(closer chan struct{}, pumpID string) {
	log.Debugf("start prometheus metrics client, addr=%s, internal=%ds", mc.addr, mc.interval)

	for {
		select {
		case <-closer:
			log.Info("metricClient exit Start")
			return
		case <-time.After(time.Duration(mc.interval) * time.Second):
			log.Info("push metrics")
			err := push.AddFromGatherer(
				"binlog",
				map[string]string{"instance": pumpID},
				mc.addr,
				prometheus.DefaultGatherer,
			)
			log.Info("end push metrics")
			if err != nil {
				log.Errorf("could not push metrics to Prometheus Pushgateway: %v", err)
			}
		}
	}
}
