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

package translator

import (
	"fmt"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

type testPbSuite struct {
	BinlogGenrator
}

var _ = check.Suite(&testPbSuite{})

func (t *testPbSuite) TestDDL(c *check.C) {
	t.SetDDL()

	pbBinog, err := TiBinlogToPbBinlog(t, t.Schema, t.Table, t.TiBinlog, nil)
	c.Assert(err, check.IsNil)

	c.Log("get ddl: ", string(pbBinog.GetDdlQuery()))
	expected := fmt.Sprintf("use %s; %s;", t.Schema, string(t.TiBinlog.GetDdlQuery()))
	c.Assert(pbBinog, check.DeepEquals, &pb.Binlog{
		Tp:       pb.BinlogType_DDL,
		CommitTs: t.TiBinlog.GetCommitTs(),
		DdlQuery: []byte(expected),
	})

	// test create database should not contains `use db`
	t.TiBinlog.DdlQuery = []byte("create database test")
	pbBinog, err = TiBinlogToPbBinlog(t, t.Schema, t.Table, t.TiBinlog, nil)
	c.Assert(err, check.IsNil)

	c.Log("get ddl: ", string(pbBinog.GetDdlQuery()))
	expected = fmt.Sprintf("%s;", string(t.TiBinlog.GetDdlQuery()))
	c.Assert(pbBinog, check.DeepEquals, &pb.Binlog{
		Tp:       pb.BinlogType_DDL,
		CommitTs: t.TiBinlog.GetCommitTs(),
		DdlQuery: []byte(expected),
	})
}

func (t *testPbSuite) testDML(c *check.C, tp pb.EventType) {
	pbBinlog, err := TiBinlogToPbBinlog(t, t.Schema, t.Table, t.TiBinlog, t.PV)
	c.Assert(err, check.IsNil)

	c.Assert(pbBinlog.GetCommitTs(), check.Equals, t.TiBinlog.GetCommitTs())
	c.Assert(pbBinlog.Tp, check.Equals, pb.BinlogType_DML)

	event := pbBinlog.DmlData.Events[0]
	c.Assert(event.GetTp(), check.Equals, tp)

	var oldDatums []types.Datum
	if tp == pb.EventType_Update {
		oldDatums = t.getOldDatums()
	}
	checkPbColumns(c, event.Row, t.getDatums(), oldDatums)
}

func (t *testPbSuite) TestInsert(c *check.C) {
	t.SetInsert(c)

	t.testDML(c, pb.EventType_Insert)
}

func (t *testPbSuite) TestUpdate(c *check.C) {
	t.SetUpdate(c)

	t.testDML(c, pb.EventType_Update)
}

func (t *testPbSuite) TestDelete(c *check.C) {
	t.SetDelete(c)

	t.testDML(c, pb.EventType_Delete)
}

func checkPbColumn(c *check.C, tp byte, pbDatum types.Datum, tiDatum types.Datum) {
	pbStr, err := pbDatum.ToString()
	c.Assert(err, check.IsNil)

	tiStr, err := tiDatum.ToString()
	c.Assert(err, check.IsNil)

	if tp == mysql.TypeEnum {
		tiStr = fmt.Sprintf("%d", tiDatum.GetInt64())
	}

	c.Assert(pbStr, check.Equals, tiStr)
}

func checkPbColumns(c *check.C, cols [][]byte, datums []types.Datum, oldDatums []types.Datum) {
	for i := 0; i < len(cols); i++ {
		col := &pb.Column{}
		err := col.Unmarshal(cols[i])
		c.Assert(err, check.IsNil)

		_, datum, err := codec.DecodeOne(col.Value)
		c.Assert(err, check.IsNil)
		checkPbColumn(c, col.Tp[0], datum, datums[i])

		if oldDatums != nil {
			_, oldDatum, err := codec.DecodeOne(col.ChangedValue)
			c.Assert(err, check.IsNil)
			checkPbColumn(c, col.Tp[0], oldDatum, oldDatums[i])
		}
	}
}
