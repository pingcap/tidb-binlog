package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// TxnHistogram means transaction execution lantency histogram.
	TxnHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "reparo",
			Name:      "txn_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})
	WaitDMLExecutedHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "reparo",
			Name:      "wait_dml_executed",
			Help:      "Bucketed histogram of processing time(s) of waiting DML executed before DDL executed",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})
	WaitDDLExecutedHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "reparo",
			Name:      "wait_ddl_executed",
			Help:      "Bucketed histogram of processing time(s) of waiting ddl executed",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})
	AddJobHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "reparo",
			Name:      "add_job_latency",
			Help:      "Bucketed histogram of processing time(s) of adding job",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})
	ResolveCausalityHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "reparo",
			Name:      "resolve_causality",
			Help:      "Bucketed histogram of processing time(s) of resolving causality",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		})

	// Total
)

func init() {
	prometheus.MustRegister(TxnHistogram)
	prometheus.MustRegister(WaitDMLExecutedHistogram)
	prometheus.MustRegister(WaitDDLExecutedHistogram)
	prometheus.MustRegister(AddJobHistogram)
	prometheus.MustRegister(ResolveCausalityHistogram)
}
