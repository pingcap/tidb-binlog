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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	slowCatchUpThreshold = time.Second
	recoveryCoolDown     = time.Minute
)

type slowChaser struct {
	on                 int32
	vlog               *valueLog
	lastUnreadPtr      *valuePointer
	recoveryTimeout    time.Duration
	lastRecoverAttempt time.Time
	output             chan *request
	WriteLock          sync.Mutex
}

func newSlowChaser(vlog *valueLog, recoveryTimeout time.Duration, output chan *request) *slowChaser {
	return &slowChaser{
		recoveryTimeout: recoveryTimeout,
		vlog:            vlog,
		output:          output,
	}
}

func (sc *slowChaser) IsOn() bool {
	return atomic.LoadInt32(&sc.on) == 1
}

func (sc *slowChaser) TurnOn(lastUnreadPtr *valuePointer) {
	sc.lastUnreadPtr = lastUnreadPtr
	atomic.StoreInt32(&sc.on, 1)
	log.Info("Slow chaser turned on")
	slowChaserCount.WithLabelValues("turned_on").Add(1.0)
}

func (sc *slowChaser) turnOff() {
	atomic.StoreInt32(&sc.on, 0)
	sc.lastUnreadPtr = nil
	log.Info("Slow chaser turned off")
	slowChaserCount.WithLabelValues("turned_off").Add(1.0)
}

func (sc *slowChaser) Run(ctx context.Context) {
	for {
		if canceled := sc.waitUntilTurnedOn(ctx, 500*time.Millisecond); canceled {
			log.Info("Slow chaser quits")
			return
		}

		if sc.lastUnreadPtr == nil {
			log.Error("lastUnreadPtr should never be nil when slowChaser is on")
			continue
		}

		t0 := time.Now()
		err := sc.catchUp()
		if err != nil {
			log.Error("Failed to catch up", zap.Error(err))
			continue
		}
		tCatchUp := time.Since(t0)
		slowChaserCatchUpTimeHistogram.Observe(tCatchUp.Seconds())
		isSlowCatchUp := tCatchUp >= slowCatchUpThreshold
		hasRecentRecoverAttempt := time.Since(sc.lastRecoverAttempt) <= recoveryCoolDown

		if isSlowCatchUp || hasRecentRecoverAttempt {
			log.Info(
				"Skip recovery for now",
				zap.Bool("slow catch up", isSlowCatchUp),
				zap.Bool("recently attempted recovery", hasRecentRecoverAttempt),
			)
			continue
		}

		sc.lastRecoverAttempt = time.Now()
		// Try to recover from slow mode in a limited time
		// Once we hold the write lock, we can be sure the vlog is not being appended
		sc.WriteLock.Lock()
		slowChaserCount.WithLabelValues("recovery").Add(1.0)
		log.Info("Stopped writing temporarily to recover from slow mode")
		// Try to catch up with scanning again, if this succeeds, we can be sure
		// that all vlogs have been sent to the downstream, and it's safe to turn
		// off the slow chaser
		err = sc.catchUpWithTimeout(sc.recoveryTimeout)
		if err != nil {
			log.Error("Failed to recover from slow mode", zap.Error(err))
			sc.WriteLock.Unlock()
			continue
		}
		sc.turnOff()
		sc.WriteLock.Unlock()
		log.Info("Successfully recover from slow mode")
	}
}

func (sc *slowChaser) catchUp() error {
	slowChaserCount.WithLabelValues("catch_up").Add(1.0)
	log.Info("Scanning requests to catch up with vlog", zap.Any("start", sc.lastUnreadPtr))
	count := 0
	err := sc.vlog.scanRequests(*sc.lastUnreadPtr, func(req *request) error {
		sc.lastUnreadPtr = &req.valuePointer
		sc.output <- req
		count++
		return nil
	})
	log.Info("Finish scanning vlog", zap.Int("processed", count))
	return errors.Trace(err)
}

func (sc *slowChaser) catchUpWithTimeout(timeout time.Duration) error {
	log.Info("Scanning requests to recover", zap.Any("start", sc.lastUnreadPtr))
	errTimeout := errors.New("Recovery Timeout")
	t0 := time.Now()
	err := sc.vlog.scanRequests(*sc.lastUnreadPtr, func(req *request) error {
		if time.Since(t0) >= timeout {
			return errTimeout
		}
		sc.lastUnreadPtr = &req.valuePointer
		timeLeft := sc.recoveryTimeout - time.Since(t0)
		select {
		case sc.output <- req:
		case <-time.After(timeLeft):
			// Return a custom error here to stop scanning
			return errTimeout
		}
		return nil
	})
	return err
}

func (sc *slowChaser) waitUntilTurnedOn(ctx context.Context, checkInterval time.Duration) (canceled bool) {
	// It should be OK to check periodically here,
	// because compared to scanning, the overhead introduced by
	// sleeping and waking up is trivial.
	// And it's less error prone than using sync.Cond.
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
	for !sc.IsOn() {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return true
		}
	}
	return false
}
