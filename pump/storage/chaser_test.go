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
	"context"
	"time"

	. "github.com/pingcap/check"
)

type waitUntilTurnedOnSuite struct{}

var _ = Suite(&waitUntilTurnedOnSuite{})

func (s *waitUntilTurnedOnSuite) TestShouldStopWhenCanceled(c *C) {
	sc := slowChaser{}
	ctx, cancel := context.WithCancel(context.Background())
	signal := make(chan struct{})
	checkInterval := time.Second
	go func() {
		err := sc.waitUntilTurnedOn(ctx, checkInterval)
		c.Assert(err, Equals, context.Canceled)
		close(signal)
	}()

	cancel()

	select {
	case <-signal:
	case <-time.After(checkInterval):
		c.Fatal("Wait doesn't stop in time after canceled.")
	}
}

func (s *waitUntilTurnedOnSuite) TestShouldReturnWhenTurnedOn(c *C) {
	sc := slowChaser{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signal := make(chan struct{})
	checkInterval := time.Second
	go func() {
		err := sc.waitUntilTurnedOn(ctx, checkInterval)
		c.Assert(err, IsNil)
		close(signal)
	}()

	sc.TurnOn(nil)
	select {
	case <-signal:
	case <-time.After(checkInterval):
		c.Fatal("Wait doesn't stop in time after turned on.")
	}
}

type runSuite struct{}

var _ = Suite(&runSuite{})

func (rs *runSuite) TestCanBeStoppedWhenWaiting(c *C) {
	sc := slowChaser{}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		sc.Run(ctx)
		close(stopped)
	}()
	cancel()
	select {
	case <-stopped:
	case <-time.After(500 * time.Millisecond):
		c.Fatal("Takes too long to stop slow chaser")
	}
}
