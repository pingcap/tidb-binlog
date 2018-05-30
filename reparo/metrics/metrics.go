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
	// WaitExecutedHistogram is histogram of waiting dml execution duration
	WaitExecutedHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "reparo",
			Name:      "wait_dml_executed",
			Help:      "Bucketed histogram of processing time(s) of waiting DML executed before DDL executed",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 18),
		}, []string{"type"})
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
	prometheus.MustRegister(WaitExecutedHistogram)
	prometheus.MustRegister(AddJobHistogram)
	prometheus.MustRegister(ExecuteTotalCounter)
}
