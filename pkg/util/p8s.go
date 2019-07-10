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

package util

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"go.uber.org/zap"
)

var (
	addToPusher = addFromGatherer
)

// NewMetricClient returns a pointer to a MetricClient
func NewMetricClient(addr string, interval time.Duration, registry *prometheus.Registry) *MetricClient {
	return &MetricClient{addr: addr, interval: interval, registry: registry}
}

// MetricClient manage the periodic push to the Prometheus Pushgateway.
type MetricClient struct {
	addr     string
	interval time.Duration
	registry *prometheus.Registry
}

// Start run a loop of pushing metrics to Prometheus Pushgateway.
func (mc MetricClient) Start(ctx context.Context, grouping map[string]string) {
	log.Debug("Start prometheus metrics client",
		zap.String("addr", mc.addr),
		zap.Float64("interval second", mc.interval.Seconds()),
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(mc.interval):
			if err := addToPusher("binlog", grouping, mc.addr, mc.registry); err != nil {
				log.Error("push metrics to Prometheus Pushgateway failed", zap.Error(err))
			}
		}
	}
}

func addFromGatherer(job string, grouping map[string]string, url string, g prometheus.Gatherer) error {
	pusher := push.New(url, job)
	// add grouping
	for k, v := range grouping {
		pusher = pusher.Grouping(k, v)
	}
	pusher = pusher.Gatherer(g)
	return pusher.Add()
}
