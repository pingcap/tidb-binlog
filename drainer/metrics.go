package drainer

import (
	"time"

	"github.com/ngaut/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"golang.org/x/net/context"
)

var (
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
	prometheus.MustRegister(positionGauge)
	prometheus.MustRegister(eventCounter)
	prometheus.MustRegister(txnHistogram)
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
			err := push.FromGatherer(
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
