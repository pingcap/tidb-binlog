package reparo

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
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

	readBinlogSizeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "read_binlog_size",
			Help:      "Bucketed histogram of size of a binlog.",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 25),
		})

	queueSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "drainer",
			Name:      "queue_size",
			Help:      "the size of queue",
		}, []string{"name"})
)

func init() {
	registry := prometheus.DefaultRegisterer
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector())
	registry.MustRegister(checkpointTSOGauge)
	registry.MustRegister(eventCounter)
	registry.MustRegister(executeHistogram)
	registry.MustRegister(readBinlogSizeHistogram)
	registry.MustRegister(queryHistogramVec)
	registry.MustRegister(queueSizeGauge)

	if gatherer, ok := registry.(prometheus.Gatherer); ok {
		prometheus.DefaultGatherer = gatherer
	}
}
