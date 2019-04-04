package arbiter

import (
	"context"
	"time"

	. "github.com/pingcap/check"
)

type testStartSuite struct{}

var _ = Suite(&testStartSuite{})

func (s *testStartSuite) TestCanBeStoppedFromOutside(c *C) {
	mc := metricClient{addr: "localhost", interval: 2}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	signal := make(chan struct{})
	go func() {
		select {
		case <-time.After(1 * time.Second):
			c.Fatal("Metric collection doesn't stop in time")
		case <-signal:
			return
		}
	}()
	mc.Start(ctx, 1234)
	close(signal)
}
