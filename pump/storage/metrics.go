package storage

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	gcTSGause = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "gc_ts",
			Help:      "gc ts of storage",
		})

	storageSizeGause = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "storage_size_bytes",
			Help:      "storage size info",
		}, []string{"type"})

	maxCommitTSGause = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "max_commit_ts",
			Help:      "max commit ts of storage",
		})

	tikvQueryCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "query_tikv_count",
			Help:      "Total count that queried tikv.",
		})

	errorCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "error_count",
			Help:      "Total error count in storage",
		}, []string{"type"})

	writeBinlogSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "write_binlog_size",
			Help:      "write binlog size",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 25),
		}, []string{"type"})

	writeBinlogTimeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "write_binlog_duration_time",
			Help:      "Bucketed histogram of write time (s) of  binlog.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 20),
		}, []string{"type"})
)

func init() {
	prometheus.MustRegister(gcTSGause)
	prometheus.MustRegister(maxCommitTSGause)
	prometheus.MustRegister(tikvQueryCount)
	prometheus.MustRegister(errorCount)
	prometheus.MustRegister(writeBinlogSizeHistogram)
	prometheus.MustRegister(writeBinlogTimeHistogram)
	prometheus.MustRegister(storageSizeGause)
}
