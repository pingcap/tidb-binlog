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

package sync

import (
	"sync"
	"time"

	"github.com/pingcap/check"
)

type msgTrackerSuite struct{}

var _ = check.Suite(&msgTrackerSuite{})

func (ts *msgTrackerSuite) TestShouldBeThreadSafe(c *check.C) {
	tracker := newMsgTracker()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var i int64
		for ; i < 10; i++ {
			tracker.Sent(i)
			tracker.Sent(i)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		var i int64
		for ; i < 10; i++ {
			tracker.Acked(i)
		}
	}()
	wg.Wait()
	c.Assert(tracker.HasPending(), check.IsTrue)
	var i int64
	for ; i < 10; i++ {
		tracker.Acked(i)
	}
	c.Assert(tracker.HasPending(), check.IsFalse)
}

func (ts *msgTrackerSuite) TestWaitingTooLongForAck(c *check.C) {
	tracker := newMsgTracker()
	tracker.Sent(1)
	c.Assert(tracker.HasWaitedTooLongForAck(time.Second), check.IsFalse)
	tracker.Acked(1)
	time.Sleep(10 * time.Nanosecond)
	c.Assert(tracker.HasWaitedTooLongForAck(time.Nanosecond), check.IsFalse)
	tracker.Sent(2)
	time.Sleep(10 * time.Microsecond)
	c.Assert(tracker.HasWaitedTooLongForAck(time.Millisecond), check.IsFalse)
}
