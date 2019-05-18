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
	"path"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"go.uber.org/zap/zapcore"
)

type logSuite struct{}

var _ = Suite(&logSuite{})

func (s *logSuite) TestLog(c *C) {
	logger := NewLog()
	logger.Add("drainer", time.Millisecond)
	t0 := time.Now()

	var counter int
	callback := func() {
		counter++
	}

	for i := 0; i < 1000; i++ {
		logger.Print("drainer", callback)
	}

	nInterval := time.Since(t0) / time.Millisecond

	c.Assert(counter, Equals, int(nInterval)+1)
}

func (s *logSuite) TestInitLogger(c *C) {
	f := path.Join(c.MkDir(), "test")
	err := InitLogger("error", f)
	c.Assert(err, IsNil)
	c.Assert(log.GetLevel(), Equals, zapcore.ErrorLevel)
}
