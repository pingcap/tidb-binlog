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

package reparo

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path"
	"runtime"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/filter"
)

type testConfigSuite struct{}

var _ = check.Suite(&testConfigSuite{})

func (s *testConfigSuite) TestParseTemplateConfig(c *check.C) {
	config := NewConfig()

	arg := fmt.Sprintf("-config=%s", getTemplateConfigFilePath())
	err := config.Parse([]string{arg})
	c.Assert(err, check.IsNil, check.Commentf("arg: %s", arg))
}

func (s *testConfigSuite) TestTSORangeParsing(c *check.C) {
	config := NewConfig()

	err := config.Parse([]string{
		"-data-dir=/tmp/data",
		"-start-datetime=2019-01-01 15:07:00",
		"-stop-datetime=2019-02-01 15:07:00",
	})
	c.Assert(err, check.IsNil)
	c.Assert(config.StartTSO, check.Not(check.Equals), 0)
	c.Assert(config.StopTSO, check.Not(check.Equals), 0)
}

func (s *testConfigSuite) TestDateTimeToTSO(c *check.C) {
	_, err := dateTimeToTSO("123123")
	c.Assert(err, check.NotNil)
	_, err = dateTimeToTSO("2019-02-02 15:07:05")
	c.Assert(err, check.IsNil)
}

func (s *testConfigSuite) TestAdjustDoDBAndTable(c *check.C) {
	config := &Config{}
	config.DoTables = []filter.TableName{
		{
			Schema: "TEST1",
			Table:  "tablE1",
		},
	}
	config.DoDBs = []string{"TEST1", "test2"}

	config.adjustDoDBAndTable()

	c.Assert(config.DoTables[0].Schema, check.Equals, "test1")
	c.Assert(config.DoTables[0].Table, check.Equals, "table1")
	c.Assert(config.DoDBs[0], check.Equals, "test1")
	c.Assert(config.DoDBs[1], check.Equals, "test2")
}

func (s *testConfigSuite) TestParseConfigFileWithInvalidArgs(c *check.C) {
	yc := struct {
		Dir                    string `toml:"data-dir" json:"data-dir"`
		StartDatetime          string `toml:"start-datetime" json:"start-datetime"`
		StopDatetime           string `toml:"stop-datetime" json:"stop-datetime"`
		StartTSO               int64  `toml:"start-tso" json:"start-tso"`
		StopTSO                int64  `toml:"stop-tso" json:"stop-tso"`
		LogFile                string `toml:"log-file" json:"log-file"`
		LogLevel               string `toml:"log-level" json:"log-level"`
		UnrecognizedOptionTest bool   `toml:"unrecognized-option-test" json:"unrecognized-option-test"`
	}{
		"/tmp/reparo",
		"",
		"",
		0,
		0,
		"tmp/reparo/reparo.log",
		"debug",
		true,
	}

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(yc)
	c.Assert(err, check.IsNil)

	configFilename := path.Join(c.MkDir(), "reparo_config_invalid.toml")
	err = ioutil.WriteFile(configFilename, buf.Bytes(), 0644)
	c.Assert(err, check.IsNil)

	args := []string{
		"--config",
		configFilename,
	}

	cfg := NewConfig()
	err = cfg.Parse(args)
	c.Assert(err, check.ErrorMatches, ".*contained unknown configuration options: unrecognized-option-test.*")
}

func getTemplateConfigFilePath() string {
	// we put the template config file in "cmd/reapro/reparo.toml"
	_, filename, _, _ := runtime.Caller(0)
	path := path.Join(path.Dir(filename), "../cmd/reparo/reparo.toml")

	return path
}
