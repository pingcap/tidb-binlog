// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	gcTSGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "gc_ts",
			Help:      "gc ts of storage",
		})

	storageSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "pump_storage",
			Name:      "storage_size_bytes",
			Help:      "storage size info",
		}, []string{"type"})

	maxCommitTSGauge = prometheus.NewGauge(
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

// InitMetircs register the metrics to registry
func InitMetircs(registry *prometheus.Registry) {
	registry.MustRegister(gcTSGauge)
	registry.MustRegister(maxCommitTSGauge)
	registry.MustRegister(tikvQueryCount)
	registry.MustRegister(errorCount)
	registry.MustRegister(writeBinlogSizeHistogram)
	registry.MustRegister(writeBinlogTimeHistogram)
	registry.MustRegister(storageSizeGauge)
}
