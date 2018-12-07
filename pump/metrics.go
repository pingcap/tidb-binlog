package pump

import (
	"os"
	"time"

	"github.com/ngaut/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"golang.org/x/net/context"
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

var registry = prometheus.NewRegistry()

func init() {
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{
		PidFn: func() (int, error) {
			return os.Getpid(), nil
		},
		Namespace: "",
	}))

	registry.MustRegister(prometheus.NewGoCollector())

	registry.MustRegister(rpcHistogram)
	registry.MustRegister(writeBinlogSizeHistogram)
	registry.MustRegister(readBinlogHistogram)
	registry.MustRegister(lossBinlogCacheCounter)
	registry.MustRegister(writeBinlogHistogram)
	registry.MustRegister(readErrorCounter)
	registry.MustRegister(writeErrorCounter)
	registry.MustRegister(checkpointGauge)
	registry.MustRegister(corruptionBinlogCounter)
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
			log.Info("metricClient exit Start")
			return
		case <-time.After(time.Duration(mc.interval) * time.Second):
			err := push.AddFromGatherer(
				"binlog",
				map[string]string{"instance": pumpID},
				mc.addr,
				registry,
			)
			if err != nil {
				log.Errorf("could not push metrics to Prometheus Pushgateway: %v", err)
			}
		}
	}
}
