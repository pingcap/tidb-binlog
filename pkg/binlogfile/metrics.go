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

package binlogfile

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	writeBinlogSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "binlogfile",
			Name:      "write_binlog_size",
			Help:      "write binlog size",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 20),
		}, []string{"label"})

	writeBinlogHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "binlogfile",
			Name:      "write_binlog_duration_time",
			Help:      "Bucketed histogram of write time (s) of a binlog.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		}, []string{"label"})

	readBinlogHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "binlogfile",
			Name:      "read_binlog_duration_time",
			Help:      "Bucketed histogram of read time (s) of a binlog.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		}, []string{"label"})

	corruptionBinlogCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "binlogfile",
			Name:      "corruption_binlog_count",
			Help:      "corruption binlog count",
		})
)

// InitMetircs register the metrics to registry
func InitMetircs(registry *prometheus.Registry) {
	registry.MustRegister(writeBinlogSizeHistogram)
	registry.MustRegister(readBinlogHistogram)
	registry.MustRegister(writeBinlogHistogram)
	registry.MustRegister(corruptionBinlogCounter)
}
