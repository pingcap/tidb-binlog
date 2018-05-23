package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// TxnHistogram is histogram of transaction execution duration .
	TxnHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "reparo",
			Name:      "txn_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a txn.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 18),
		})
	// WaitDMLExecutedHistogram is histogram of waiting dml execution duration
	WaitDMLExecutedHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "reparo",
			Name:      "wait_dml_executed",
			Help:      "Bucketed histogram of processing time(s) of waiting DML executed before DDL executed",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 18),
		})
	// WaitDDLExecutedHistogram is histogram of waiting ddl execution duration.
	WaitDDLExecutedHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "reparo",
			Name:      "wait_ddl_executed",
			Help:      "Bucketed histogram of processing time(s) of waiting ddl executed",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 18),
		})
	// AddJobHistogram is histogram of adding job to channel(queue).
	AddJobHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "reparo",
			Name:      "add_job_latency",
			Help:      "Bucketed histogram of processing time(s) of adding job",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		})
	// ExecuteTotalCounter is counter of total execution.
	ExecuteTotalCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "reparo",
			Name:      "execute_total",
			Help:      "counter of total execution",
		})
)

func init() {
	prometheus.MustRegister(TxnHistogram)
	prometheus.MustRegister(WaitDMLExecutedHistogram)
	prometheus.MustRegister(WaitDDLExecutedHistogram)
	prometheus.MustRegister(AddJobHistogram)
	prometheus.MustRegister(ExecuteTotalCounter)
}
