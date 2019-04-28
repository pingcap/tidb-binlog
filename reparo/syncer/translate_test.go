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

package syncer

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

func Test(t *testing.T) { check.TestingT(t) }

type testTranslateSuite struct{}

var _ = check.Suite(&testTranslateSuite{})

func (s *testTranslateSuite) TestPBBinlogToTxn(c *check.C) {
	tests := map[*pb.Binlog]*loader.Txn{
		{
			Tp:       pb.BinlogType_DDL,
			DdlQuery: []byte("use db1; create table table1(id int)"),
		}: {
			DDL: &loader.DDL{
				SQL: "use db1; create table table1(id int)",
			},
		},
		// TODO add dml test
		{
			Tp: pb.BinlogType_DML,
			DmlData: &pb.DMLData{
				Events: []pb.Event{},
			},
		}: {},
	}

	for binlog, txn := range tests {
		getTxn, err := pbBinlogToTxn(binlog)
		c.Assert(err, check.IsNil)
		c.Assert(getTxn, check.DeepEquals, txn)
	}
}
