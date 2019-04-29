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
	"fmt"
	"path"
	"runtime"

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

func getTemplateConfigFilePath() string {
	// we put the template config file in "cmd/reapro/reparo.toml"
	_, filename, _, _ := runtime.Caller(0)
	path := path.Join(path.Dir(filename), "../cmd/reparo/reparo.toml")

	return path
}
