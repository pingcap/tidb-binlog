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

package loopbacksync

import (
	"database/sql/driver"
	"fmt"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/check"
)

func Test(t *testing.T) { check.TestingT(t) }

type loopbackSuite struct{}

var _ = check.Suite(&loopbackSuite{})

func (s *loopbackSuite) TestNewLoopBackSyncInfo(c *check.C) {
	var ChannelID int64 = 1
	var LoopbackControl = true
	var SyncDDL = false

	l := NewLoopBackSyncInfo(ChannelID, LoopbackControl, SyncDDL, "", nil, false, "rel", "_drainer_repl_mark")

	c.Assert(l, check.DeepEquals, &LoopBackSync{
		ChannelID:       ChannelID,
		LoopbackControl: LoopbackControl,
		SyncDDL:         SyncDDL,
		PluginPath:      "",
		PluginNames:     nil,
		SupportPlugin:   false,
		MarkDBName:      "rel",
		MarkTableName:   "_drainer_repl_mark",
	})
}

func (s *loopbackSuite) TestCreateMarkTable(c *check.C) {
	db, mk, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	CreateMarkDBDDL := "create database IF NOT EXISTS rel;"
	CreateMarkTableDDL := fmt.Sprintf("CREATE TABLE If Not Exists %s.%s (%s bigint not null,%s bigint not null DEFAULT 0, %s bigint DEFAULT 0, %s varchar(64) ,PRIMARY KEY (%s,%s));", "rel", "_drainer_repl_mark", ID, ChannelID, Val, ChannelInfo, ID, ChannelID)

	mk.ExpectExec(regexp.QuoteMeta(CreateMarkDBDDL)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mk.ExpectExec(regexp.QuoteMeta(CreateMarkTableDDL)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = CreateMarkTable(db, "rel", "_drainer_repl_mark")
	c.Assert(err, check.IsNil)

	err = mk.ExpectationsWereMet()
	c.Assert(err, check.IsNil)
}

func (s *loopbackSuite) TestInitMarkTableData(c *check.C) {
	db, mk, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	var cid int64 = 1
	rowNum := 16

	var args []driver.Value
	for i := 0; i < rowNum; i++ {
		args = append(args, i, cid, 1 /*value*/, "" /*channel_info*/)
	}
	mk.ExpectExec("REPLACE INTO .*").WithArgs(args...).
		WillReturnResult(sqlmock.NewResult(0, int64(rowNum)))

	err = InitMarkTableData(db, rowNum, cid)
	c.Assert(err, check.IsNil)

	err = mk.ExpectationsWereMet()
	c.Assert(err, check.IsNil)
}

func (s *loopbackSuite) TestCleanMarkTableData(c *check.C) {
	db, mk, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	var cid int64 = 1
	mk.ExpectExec("delete from .*").WithArgs(cid).WillReturnResult(sqlmock.NewResult(0, 1))

	err = CleanMarkTableData(db, cid)
	c.Assert(err, check.IsNil)

	err = mk.ExpectationsWereMet()
	c.Assert(err, check.IsNil)
}

func (s *loopbackSuite) TestUpdateMark(c *check.C) {
	db, mk, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	mk.ExpectBegin()
	tx, err := db.Begin()
	c.Assert(err, check.IsNil)

	var id int64 = 1
	var cid int64 = 1
	mk.ExpectExec("update .*").WithArgs(id, cid).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = UpdateMark(tx, id, cid)
	c.Assert(err, check.IsNil)

	err = mk.ExpectationsWereMet()
	c.Assert(err, check.IsNil)
}
