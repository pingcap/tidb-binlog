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

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/check"
	obinlog "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
	"github.com/pingcap/tidb/types"
)

type testKafkaSuite struct {
	BinlogGenrator
}

var _ = check.Suite(&testKafkaSuite{})

func (t *testKafkaSuite) TestDDL(c *check.C) {
	t.SetDDL()

	slaveBinog, err := TiBinlogToSlaveBinlog(t, t.Schema, t.Table, t.TiBinlog, nil)
	c.Assert(err, check.IsNil)

	c.Assert(slaveBinog, check.DeepEquals, &obinlog.Binlog{
		Type:     obinlog.BinlogType_DDL,
		CommitTs: t.TiBinlog.GetCommitTs(),
		DdlData: &obinlog.DDLData{
			SchemaName: proto.String(t.Schema),
			TableName:  proto.String(t.Table),
			DdlQuery:   t.TiBinlog.GetDdlQuery(),
		},
	})
}

func (t *testKafkaSuite) testDML(c *check.C, tp obinlog.MutationType) {
	slaveBinog, err := TiBinlogToSlaveBinlog(t, t.Schema, t.Table, t.TiBinlog, t.PV)
	c.Assert(err, check.IsNil)

	c.Assert(slaveBinog.GetCommitTs(), check.Equals, t.TiBinlog.GetCommitTs())
	c.Assert(slaveBinog.Type, check.Equals, obinlog.BinlogType_DML)

	table := slaveBinog.DmlData.Tables[0]
	tableMut := table.Mutations[0]
	c.Assert(tableMut.GetType(), check.Equals, tp)

	checkColumns(c, table.ColumnInfo, tableMut.Row.Columns, t.getDatums())
	if tp == obinlog.MutationType_Update {
		checkColumns(c, table.ColumnInfo, tableMut.ChangeRow.Columns, t.getOldDatums())
	}
}

func (t *testKafkaSuite) TestInsert(c *check.C) {
	t.SetInsert(c)

	t.testDML(c, obinlog.MutationType_Insert)
}

func (t *testKafkaSuite) TestUpdate(c *check.C) {
	t.SetUpdate(c)

	t.testDML(c, obinlog.MutationType_Update)
}

func (t *testKafkaSuite) TestDelete(c *check.C) {
	t.SetDelete(c)

	t.testDML(c, obinlog.MutationType_Delete)
}

func checkColumns(c *check.C, colInfos []*obinlog.ColumnInfo, cols []*obinlog.Column, datums []types.Datum) {
	for i := 0; i < len(cols); i++ {
		checkColumn(c, colInfos[i], cols[i], datums[i])
	}
}

func checkColumn(c *check.C, info *obinlog.ColumnInfo, col *obinlog.Column, datum types.Datum) {
	if col.GetIsNull() {
		if datum.IsNull() {
			return
		}

		c.FailNow()
	}

	// just compare by text string
	var colV string
	if col.Int64Value != nil {
		colV = fmt.Sprintf("%v", col.GetInt64Value())
	} else if col.Uint64Value != nil {
		colV = fmt.Sprintf("%v", col.GetUint64Value())
	} else if col.DoubleValue != nil {
		colV = fmt.Sprintf("%v", col.GetDoubleValue())
	} else if col.BytesValue != nil {
		colV = fmt.Sprintf("%v", col.GetBytesValue())
	} else {
		colV = fmt.Sprintf("%v", col.GetStringValue())
	}

	datumV := fmt.Sprintf("%v", datum.GetValue())
	if info.GetMysqlType() == "enum" {
		// we set uint64 as the index for slave proto but not the name
		datumV = fmt.Sprintf("%v", datum.GetInt64())
	}

	c.Assert(colV, check.Equals, datumV)
}
