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

package drainer

import (
	"errors"

	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/sql"
	"github.com/pingcap/tidb-binlog/pkg/util"
)

func (t *testDrainerSuite) TestIgnoreDDLError(c *C) {
	// test non-mysqltype error
	err := errors.New("test")
	ok := sql.IgnoreDDLError(err)
	c.Assert(ok, IsFalse)
	// test ignore error
	err1 := &mysql.MySQLError{
		Number:  1054,
		Message: "test",
	}
	ok = sql.IgnoreDDLError(err1)
	c.Assert(ok, IsTrue)
	// test non-ignore error
	err2 := &mysql.MySQLError{
		Number:  1052,
		Message: "test",
	}
	ok = sql.IgnoreDDLError(err2)
	c.Assert(ok, IsFalse)

}

type taskGroupSuite struct{}

var _ = Suite(&taskGroupSuite{})

func (s *taskGroupSuite) TestShouldRecoverFromPanic(c *C) {
	var logHook util.LogHook
	logHook.SetUp()
	defer logHook.TearDown()

	var called bool
	var g taskGroup
	g.GoNoPanic("test", func() {
		called = true
		panic("Evil Smile")
	})
	g.Wait()
	c.Assert(called, IsTrue)
	c.Assert(logHook.Entrys, HasLen, 2)
	c.Assert(logHook.Entrys[0].Message, Matches, ".*Recovered.*")
	c.Assert(logHook.Entrys[1].Message, Matches, ".*Exit.*")
}
