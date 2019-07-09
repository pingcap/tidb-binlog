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

	. "github.com/pingcap/check"
	"github.com/prometheus/client_golang/prometheus"
)

type p8sSuite struct{}

var _ = Suite(&p8sSuite{})

func (s *p8sSuite) TestCanBeStopped(c *C) {
	mc := NewMetricClient("localhost:9999", time.Millisecond, prometheus.NewRegistry())
	signal := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		mc.Start(ctx, map[string]string{"instance": "drainer-1"})
		close(signal)
	}()
	cancel()
	select {
	case <-signal:
	case <-time.After(time.Second):
		c.Fatal("Doesn't stop in time")
	}
}
