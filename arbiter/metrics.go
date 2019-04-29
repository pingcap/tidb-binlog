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

package arbiter

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"go.uber.org/zap"
)

var (
	checkpointTSOGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "arbiter",
			Name:      "checkpoint_tso",
			Help:      "save checkpoint tso of arbiter.",
		})

	queryHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "arbiter",
			Name:      "query_duration_time",
			Help:      "Bucketed histogram of processing time (s) of a query to sync data to downstream.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		}, []string{"type"})

	eventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "binlog",
			Subsystem: "arbiter",
			Name:      "event",
			Help:      "the count of sql event(dml, ddl).",
		}, []string{"type"})

	queueSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "binlog",
			Subsystem: "arbiter",
			Name:      "queue_size",
			Help:      "the size of queue",
		}, []string{"name"})

	txnLatencySecondsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "binlog",
			Subsystem: "arbiter",
			Name:      "txn_latency_seconds",
			Help:      "Bucketed histogram of seconds of a txn between loaded to downstream and committed at upstream.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 20),
		})
)

// Registry is the metrics registry of server
var Registry = prometheus.NewRegistry()

type metricClient struct {
	addr     string
	interval int
}

// Start run a loop of pushing metrics to Prometheus Pushgateway.
func (mc *metricClient) Start(ctx context.Context, port int) {
	log.Debug("start prometheus metrics client", zap.String("addr", mc.addr), zap.Int("interval second", mc.interval))
	for {
		select {
		case <-ctx.Done():
			log.Info("stop push metrics")
			return
		case <-time.After(time.Duration(mc.interval) * time.Second):
			err := push.AddFromGatherer(
				"binlog",
				map[string]string{"instance": instanceName(port)},
				mc.addr,
				Registry,
			)
			if err != nil {
				log.Error("could not push metrics to Prometheus Pushgateway", zap.Error(err))
			}
		}
	}
}

func init() {
	Registry.MustRegister(prometheus.NewProcessCollector(os.Getpid(), ""))
	Registry.MustRegister(prometheus.NewGoCollector())

	Registry.MustRegister(checkpointTSOGauge)
	Registry.MustRegister(queryHistogramVec)
	Registry.MustRegister(eventCounter)
	Registry.MustRegister(queueSizeGauge)
	Registry.MustRegister(txnLatencySecondsHistogram)
}

func instanceName(port int) string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return fmt.Sprintf("%s_%d", hostname, port)
}
