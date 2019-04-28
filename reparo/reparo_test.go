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
	"testing"

	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/filter"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb-binlog/reparo/syncer"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testReparoSuite{})

type testReparoSuite struct{}

func (s *testReparoSuite) TestIsAcceptableBinlog(c *C) {
	cases := []struct {
		startTs  int64
		endTs    int64
		binlog   *pb.Binlog
		expected bool
	}{
		{
			startTs: 0,
			endTs:   0,
			binlog: &pb.Binlog{
				CommitTs: 1518003282,
			},
			expected: true,
		}, {
			startTs: 1518003281,
			endTs:   0,
			binlog: &pb.Binlog{
				CommitTs: 1518003282,
			},
			expected: true,
		}, {
			startTs: 1518003283,
			endTs:   0,
			binlog: &pb.Binlog{
				CommitTs: 1518003282,
			},
			expected: false,
		}, {
			startTs: 0,
			endTs:   1518003283,
			binlog: &pb.Binlog{
				CommitTs: 1518003282,
			},
			expected: true,
		}, {
			startTs: 0,
			endTs:   1518003281,
			binlog: &pb.Binlog{
				CommitTs: 1518003282,
			},
			expected: false,
		}, {
			startTs: 1518003281,
			endTs:   1518003283,
			binlog: &pb.Binlog{
				CommitTs: 1518003282,
			},
			expected: true,
		},
	}

	for _, t := range cases {
		res := isAcceptableBinlog(t.binlog, t.startTs, t.endTs)
		c.Assert(res, Equals, t.expected)
	}
}

func (s *testReparoSuite) TestFilterBinlog(c *C) {
	// just check the ddl binlog and dml with db name "ignore_db" will be filtered
	afilter := filter.NewFilter([]string{"ignore_db"}, nil, nil, nil)

	ddlBinlogs := map[*pb.Binlog]bool{
		{
			Tp:       pb.BinlogType_DDL,
			DdlQuery: []byte("use ignore_db; create table a(id int)"),
		}: true,
		{
			Tp:       pb.BinlogType_DDL,
			DdlQuery: []byte("use do_db; create table a(id int)"),
		}: false,
	}

	for binlog, ignore := range ddlBinlogs {
		getIgnore, err := filterBinlog(afilter, binlog)
		c.Assert(err, IsNil)
		c.Assert(getIgnore, Equals, ignore)
	}

	dmlBinlogs := map[*pb.Binlog]bool{
		{
			Tp: pb.BinlogType_DML,
			DmlData: &pb.DMLData{
				Events: []pb.Event{{SchemaName: proto.String("ignore_db")}},
			},
		}: true,
		{
			Tp: pb.BinlogType_DML,
			DmlData: &pb.DMLData{
				Events: []pb.Event{{SchemaName: proto.String("do_db")}},
			},
		}: false,
		{
			Tp: pb.BinlogType_DML,
			DmlData: &pb.DMLData{
				Events: []pb.Event{
					{SchemaName: proto.String("do_db")},
					{SchemaName: proto.String("ignore_db")},
				}},
		}: false,
	}

	for binlog, ignore := range dmlBinlogs {
		getIgnore, err := filterBinlog(afilter, binlog)
		c.Assert(err, IsNil)
		c.Assert(getIgnore, Equals, ignore)

		// should no schema with ignore_db
		for _, e := range binlog.DmlData.GetEvents() {
			if e.GetSchemaName() == "ignore_db" {
				c.Fail()
			}
		}
	}

}

func (s *testReparoSuite) TestProcess(c *C) {
	config := NewConfig()
	dir := c.MkDir()
	binlogs := writeBinlogsInDir(dir, c)

	var args []string
	arg := fmt.Sprintf("-config=%s", getTemplateConfigFilePath())
	args = append(args, arg)
	args = append(args, fmt.Sprintf("-data-dir=%s", dir))
	args = append(args, fmt.Sprintf("-dest-type=memory"))

	err := config.Parse(args)
	c.Assert(err, IsNil, Commentf("arg: %s", arg))

	repora, err := New(config)
	c.Assert(err, IsNil)

	err = repora.Process()
	c.Assert(err, IsNil)

	memSyncer := repora.syncer.(*syncer.MemSyncer)
	c.Assert(memSyncer.GetBinlogs(), DeepEquals, binlogs)
}
