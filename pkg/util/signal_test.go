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
	"bytes"
	"log"
	"os"
	"strings"
	"syscall"
	"time"

	. "github.com/pingcap/check"
)

type signalSuite struct{}

var _ = Suite(&signalSuite{})

func (s *signalSuite) TestShouldCallFunc(c *C) {
	received := make(chan os.Signal, 2)

	SetupSignalHandler(func(s os.Signal) {
		received <- s
	})

	pid := syscall.Getpid()
	err := syscall.Kill(pid, syscall.SIGINT)
	c.Assert(err, IsNil)

	select {
	case s := <-received:
		c.Assert(s, Equals, syscall.SIGINT, Commentf("Received wrong signal"))
	case <-time.After(2 * time.Second):
		c.Fatal("Timeout waiting for the signal")
	}
}

func (s *signalSuite) TestShouldDumpStack(c *C) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	SetupSignalHandler(func(s os.Signal) {})

	pid := syscall.Getpid()
	err := syscall.Kill(pid, syscall.SIGUSR1)
	c.Assert(err, IsNil)

	time.Sleep(time.Second)

	record := buf.String()
	parts := strings.Split(strings.TrimSpace(record), "\n")
	c.Assert(len(parts), Greater, 1)
	c.Assert(parts[1], Matches, ".*Got signal.*to dump.*")
}
