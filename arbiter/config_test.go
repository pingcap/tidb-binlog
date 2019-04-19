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
	"fmt"
	"path"
	"runtime"
	"strings"

	"github.com/pingcap/check"
)

type TestConfigSuite struct {
}

var _ = check.Suite(&TestConfigSuite{})

func (t *TestConfigSuite) TestAdjustConfig(c *check.C) {
	config := Config{
		Up: UpConfig{},
		Down: DownConfig{},
	}
	config.adjustConfig()
	c.Assert(config.Up.KafkaAddrs, check.Equals, defaultKafkaAddrs)
	c.Assert(config.Up.KafkaVersion, check.Equals, defaultKafkaVersion)
	c.Assert(config.Down.Host, check.Equals, "localhost")
	c.Assert(config.Down.Port, check.Equals, 3306)
	c.Assert(config.Down.User, check.Equals, "root")
}

func (t *TestConfigSuite) TestParseConfig(c *check.C) {
	args := make([]string, 0, 10)

	// not set `up.topic`, invalid
	config := NewConfig()
	configFile := getTemplateConfigFilePath()
	args = append(args, fmt.Sprintf("-config=%s", configFile))
	err := config.Parse(args)
	c.Assert(err, check.Equals, errUpTopicNotSpecified)

	// set `up.topic` through command line args, valid
	config = NewConfig()
	upTopic := "topic-test"
	args = append(args, fmt.Sprintf("-up.topic=%s", upTopic))
	err = config.Parse(args)
	c.Assert(err, check.IsNil)
	// check config item
	c.Assert(config.LogLevel, check.Equals, "info")
	c.Assert(config.LogFile, check.Equals, "")
	c.Assert(config.LogRotate, check.Equals, "")
	c.Assert(config.ListenAddr, check.Equals, "127.0.0.1:8251")
	c.Assert(config.configFile, check.Equals, configFile)
	c.Assert(config.Up.KafkaAddrs, check.Equals, defaultKafkaAddrs)
	c.Assert(config.Up.KafkaVersion, check.Equals, defaultKafkaVersion)
	c.Assert(config.Up.InitialCommitTS, check.Equals, int64(0))
	c.Assert(config.Up.Topic, check.Equals, upTopic)
	c.Assert(config.Down.Host, check.Equals, "localhost")
	c.Assert(config.Down.Port, check.Equals, 3306)
	c.Assert(config.Down.User, check.Equals, "root")
	c.Assert(config.Down.Password, check.Equals, "")
	c.Assert(config.Down.WorkerCount, check.Equals, 16)
	c.Assert(config.Down.BatchSize, check.Equals, 64)
	c.Assert(config.Metrics.Addr, check.Equals, "")
	c.Assert(config.Metrics.Interval, check.Equals, 15)

	// overwrite with more command line args
	listenAddr := "127.0.0.1:8252"
	args = append(args, fmt.Sprintf("-addr=%s", listenAddr))
	logLevel := "error"
	args = append(args, fmt.Sprintf("-L=%s", logLevel))
	logFile := "arbiter.log"
	args = append(args, fmt.Sprintf("-log-file=%s", logFile))
	logRotate := "hour"
	args = append(args, fmt.Sprintf("-log-rotate=%s", logRotate))
	upInitCTS := int64(123)
	args = append(args, fmt.Sprintf("-up.initial-commit-ts=%d", upInitCTS))
	downWC := 456
	args = append(args, fmt.Sprintf("-down.worker-count=%d", downWC))
	downBS := 789
	args = append(args, fmt.Sprintf("-down.batch-size=%d", downBS))

	// parse again
	config = NewConfig()
	err = config.Parse(args)
	c.Assert(err, check.IsNil)
	// check again
	c.Assert(config.ListenAddr, check.Equals, listenAddr)
	c.Assert(config.LogLevel, check.Equals, logLevel)
	c.Assert(config.LogFile, check.Equals, logFile)
	c.Assert(config.LogRotate, check.Equals, logRotate)
	c.Assert(config.Up.InitialCommitTS, check.Equals, upInitCTS)
	c.Assert(config.Down.WorkerCount, check.Equals, downWC)
	c.Assert(config.Down.BatchSize, check.Equals, downBS)

	// simply verify json string
	c.Assert(strings.Contains(config.String(), listenAddr), check.IsTrue)
}

func getTemplateConfigFilePath() string {
	// we put the template config file in "cmd/arbiter/arbiter.toml"
	_, filename, _, _ := runtime.Caller(0)
	return path.Join(path.Dir(filename), "../cmd/arbiter/arbiter.toml")
}
