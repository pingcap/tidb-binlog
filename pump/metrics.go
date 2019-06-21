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

package pump

import (
	"os"

	"github.com/pingcap/tidb-binlog/pump/storage"
	"github.com/prometheus/client_golang/prometheus"
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

	binlogPurgedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "pump",
			Name:      "binlog_purge_count",
			Help:      "binlog purge count > 0 means some binlog was purged",
		}, []string{"drainer"})
)

var registry = prometheus.NewRegistry()

func init() {
	storage.InitMetircs(registry)

	registry.MustRegister(prometheus.NewProcessCollector(os.Getpid(), ""))
	registry.MustRegister(prometheus.NewGoCollector())

	registry.MustRegister(rpcHistogram)
	registry.MustRegister(lossBinlogCacheCounter)
}
