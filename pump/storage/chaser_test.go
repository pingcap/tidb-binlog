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
	"github.com/pingcap/errors"
)

type scannerMock struct {
	scanImpl func(valuePointer, func(*request) error) error
}

func (sm scannerMock) scanRequests(start valuePointer, f func(*request) error) error {
	return sm.scanImpl(start, f)
}

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

func (rs *runSuite) managedRun(ctx context.Context, c *C, sc *slowChaser) {
	stopped := make(chan struct{})
	go func() {
		sc.Run(ctx)
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(500 * time.Millisecond):
		c.Fatal("Run doesn't return in time after canceled.")
	}
}

func (rs *runSuite) TestCanBeStoppedWhenWaiting(c *C) {
	sc := slowChaser{}
	ctx, cancel := context.WithCancel(context.Background())
	go cancel()
	rs.managedRun(ctx, c, &sc)
}

func (rs *runSuite) TestShouldRetryIfFailedToCatchUp(c *C) {
	mock := struct{ scannerMock }{}
	var offsetRecords []valuePointer
	finished := make(chan struct{})
	mock.scanImpl = func(start valuePointer, f func(*request) error) error {
		offsetRecords = append(offsetRecords, start)
		for i := 0; i < 3; i++ {
			next := request{
				valuePointer: valuePointer{
					Fid:    start.Fid,
					Offset: start.Offset + int64(i*10),
				},
			}
			f(&next)
		}
		if len(offsetRecords) == 3 {
			close(finished)
		}
		return errors.New("fake")
	}
	sc := newSlowChaser(mock, time.Second, make(chan *request, 100))
	sc.TurnOn(&valuePointer{Fid: 42, Offset: 1000})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-finished
		cancel()

		c.Assert(len(offsetRecords), GreaterEqual, 3)
		c.Assert(offsetRecords[0], DeepEquals, valuePointer{Fid: 42, Offset: 1000})
		c.Assert(offsetRecords[1], DeepEquals, valuePointer{Fid: 42, Offset: 1020})
		c.Assert(offsetRecords[2], DeepEquals, valuePointer{Fid: 42, Offset: 1040})
	}()

	rs.managedRun(ctx, c, sc)
}

func (rs *runSuite) TestShouldNotTryRecoveryForSlowCatchUp(c *C) {
	mock := struct{ scannerMock }{}
	callCount := 0
	finished := make(chan struct{})
	mock.scanImpl = func(start valuePointer, f func(*request) error) error {
		callCount++
		if callCount == 3 {
			close(finished)
		}
		time.Sleep(time.Millisecond)
		return nil
	}

	sc := newSlowChaser(mock, time.Second, make(chan *request, 100))
	sc.TurnOn(&valuePointer{Fid: 42, Offset: 1000})

	// Make sure lastRecoverAttempt is not set
	c.Assert(sc.lastRecoverAttempt, Equals, time.Time{})

	origThres := slowCatchUpThreshold
	defer func() {
		slowCatchUpThreshold = origThres
	}()
	// Set threshold to be 0.5ms which is smaller than 1ms
	slowCatchUpThreshold = 500 * time.Microsecond

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-finished
		cancel()
		c.Assert(sc.lastRecoverAttempt, Equals, time.Time{})
		c.Assert(sc.IsOn(), IsTrue)
	}()

	rs.managedRun(ctx, c, sc)
}

func (rs *runSuite) TestShouldNotRecoverFrequently(c *C) {
	mock := struct{ scannerMock }{}
	callCount := 0
	finished := make(chan struct{})
	mock.scanImpl = func(start valuePointer, f func(*request) error) error {
		callCount++
		if callCount == 3 {
			close(finished)
		}
		return nil
	}

	sc := newSlowChaser(mock, time.Second, make(chan *request, 100))
	sc.TurnOn(&valuePointer{Fid: 42, Offset: 1000})

	// Set it up like we have just tried recovering and failed
	sc.lastRecoverAttempt = time.Now()
	lastRecoverAttempt := sc.lastRecoverAttempt

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-finished
		cancel()
		c.Assert(sc.lastRecoverAttempt, Equals, lastRecoverAttempt)
		c.Assert(sc.IsOn(), IsTrue)
	}()

	rs.managedRun(ctx, c, sc)
}

func (rs *runSuite) TestShouldRecoverAndTurnOff(c *C) {
	mock := struct{ scannerMock }{}
	callCount := 0
	finished := make(chan struct{})
	mock.scanImpl = func(start valuePointer, f func(*request) error) error {
		callCount++
		// Should be called 2 times: catch up and recover
		if callCount == 2 {
			close(finished)
		}
		return nil
	}

	sc := newSlowChaser(mock, time.Second, make(chan *request, 100))
	sc.TurnOn(&valuePointer{Fid: 42, Offset: 1000})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-finished
		cancel()
	}()
	rs.managedRun(ctx, c, sc)
	c.Assert(sc.IsOn(), IsFalse)
}

func (rs *runSuite) TestContinueCatchingUpIfRecoveryFailed(c *C) {
	mock := struct{ scannerMock }{}
	callCount := 0
	finished := make(chan struct{})
	mock.scanImpl = func(start valuePointer, f func(*request) error) error {
		callCount++
		// Arrange for at least 3 calls: catch up, failed-recovery, catch up
		if callCount == 3 {
			close(finished)
		}
		if callCount == 2 {
			return errors.New("Failed recovery")
		}
		return nil
	}

	sc := newSlowChaser(mock, time.Second, make(chan *request, 100))
	sc.TurnOn(&valuePointer{Fid: 42, Offset: 1000})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-finished
		cancel()
	}()

	rs.managedRun(ctx, c, sc)
	c.Assert(sc.IsOn(), IsTrue)
}
