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
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	ti "github.com/pingcap/tipb/go-binlog"
)

var _ TableInfoGetter = &BinlogGenerator{}

// BinlogGenerator is a test helper for generating some binlog.
type BinlogGenerator struct {
	TiBinlog *ti.Binlog
	PV       *ti.PrewriteValue
	Schema   string
	Table    string

	id2info map[int64]*model.TableInfo
	id2name map[int64][2]string

	datums    []types.Datum
	oldDatums []types.Datum
}

func (g *BinlogGenerator) reset() {
	g.TiBinlog = nil
	g.PV = nil
	g.Schema = ""
	g.Table = ""
	g.id2info = make(map[int64]*model.TableInfo)
	g.id2name = make(map[int64][2]string)
}

// SetDelete set the info to be a delete event
func (g *BinlogGenerator) SetDelete(c *check.C) {
	g.reset()
	info := g.setEvent(c)

	row := testGenDeleteBinlog(c, info, g.datums)

	g.PV.Mutations = append(g.PV.Mutations, ti.TableMutation{
		TableId:     info.ID,
		DeletedRows: [][]byte{row},
		Sequence:    []ti.MutationType{ti.MutationType_DeleteRow},
	})
}

func (g *BinlogGenerator) getDatums() (datums []types.Datum) {
	datums = g.datums
	return
}

func (g *BinlogGenerator) getOldDatums() (datums []types.Datum) {
	datums = g.oldDatums
	return
}

// TableByID implements TableInfoGetter interface
func (g *BinlogGenerator) TableByID(id int64) (info *model.TableInfo, ok bool) {
	info, ok = g.id2info[id]
	return
}

// SchemaAndTableName implements TableInfoGetter interface
func (g *BinlogGenerator) SchemaAndTableName(id int64) (schema string, table string, ok bool) {
	names, ok := g.id2name[id]
	if !ok {
		return "", "", false
	}

	schema = names[0]
	table = names[1]
	ok = true
	return
}

// IsDroppingColumn implements TableInfoGetter interface
func (g *BinlogGenerator) IsDroppingColumn(id int64) bool {
	return false
}

// SetDDL set up a ddl binlog.
func (g *BinlogGenerator) SetDDL() {
	g.reset()
	g.TiBinlog = &ti.Binlog{
		Tp:       ti.BinlogType_Commit,
		StartTs:  100,
		CommitTs: 200,
		DdlQuery: []byte("create table test(id int)"),
		DdlJobId: 1,
	}
	g.PV = nil
	g.Schema = "test"
	g.Table = "test"
}

func (g *BinlogGenerator) setEvent(c *check.C) *model.TableInfo {
	g.TiBinlog = &ti.Binlog{
		Tp:       ti.BinlogType_Commit,
		StartTs:  100,
		CommitTs: 200,
	}

	g.PV = new(ti.PrewriteValue)

	info := testGenTable("hasID")
	g.id2info[info.ID] = info
	g.id2name[info.ID] = [2]string{"test", info.Name.L}

	g.datums = testGenRandomDatums(c, info.Columns)
	g.oldDatums = testGenRandomDatums(c, info.Columns)
	c.Assert(len(g.datums), check.Equals, len(info.Columns))

	return info
}

// SetInsert set up a insert event binlog.
func (g *BinlogGenerator) SetInsert(c *check.C) {
	g.reset()
	info := g.setEvent(c)

	row := testGenInsertBinlog(c, info, g.datums)
	g.PV.Mutations = append(g.PV.Mutations, ti.TableMutation{
		TableId:      info.ID,
		InsertedRows: [][]byte{row},
		Sequence:     []ti.MutationType{ti.MutationType_Insert},
	})
}

// SetAllDML one insert/update/delete/update in one txn.
func (g *BinlogGenerator) SetAllDML(c *check.C) {
	g.reset()
	info := g.setEvent(c)

	mut := ti.TableMutation{
		TableId: info.ID,
	}

	// insert
	row := testGenInsertBinlog(c, info, g.datums)
	mut.InsertedRows = append(mut.InsertedRows, row)
	mut.Sequence = append(mut.Sequence, ti.MutationType_Insert)

	// update
	row = testGenUpdateBinlog(c, info, g.oldDatums, g.datums)
	mut.UpdatedRows = append(mut.UpdatedRows, row)
	mut.Sequence = append(mut.Sequence, ti.MutationType_Update)

	// delete
	row = testGenDeleteBinlog(c, info, g.datums)
	mut.DeletedRows = append(mut.DeletedRows, row)
	mut.Sequence = append(mut.Sequence, ti.MutationType_DeleteRow)

	g.PV.Mutations = append(g.PV.Mutations, mut)
}

// SetUpdate set up a update event binlog.
func (g *BinlogGenerator) SetUpdate(c *check.C) {
	g.reset()
	info := g.setEvent(c)

	row := testGenUpdateBinlog(c, info, g.oldDatums, g.datums)

	g.PV.Mutations = append(g.PV.Mutations, ti.TableMutation{
		TableId:     info.ID,
		UpdatedRows: [][]byte{row},
		Sequence:    []ti.MutationType{ti.MutationType_Update},
	})
}

// hasID:  create table t(id int primary key, name varchar(45), sex enum("male", "female"));
// hasPK:  create table t(id int, name varchar(45), sex enum("male", "female"), PRIMARY KEY(id, name));
// normal: create table t(id int, name varchar(45), sex enum("male", "female"));
func testGenTable(tt string) *model.TableInfo {
	t := &model.TableInfo{State: model.StatePublic}
	t.Name = model.NewCIStr("account")

	// the hard values are from TiDB :-), so just ingore them
	userIDCol := &model.ColumnInfo{
		ID:     1,
		Name:   model.NewCIStr("ID"),
		Offset: 0,
		FieldType: types.FieldType{
			Tp:      mysql.TypeLong,
			Flag:    mysql.BinaryFlag,
			Flen:    11,
			Decimal: -1,
			Charset: "binary",
			Collate: "binary",
		},
		State: model.StatePublic,
	}

	userNameCol := &model.ColumnInfo{
		ID:     2,
		Name:   model.NewCIStr("NAME"),
		Offset: 1,
		FieldType: types.FieldType{
			Tp:      mysql.TypeVarchar,
			Flag:    0,
			Flen:    45,
			Decimal: -1,
			Charset: "utf8",
			Collate: "utf8_unicode_ci",
		},
		State: model.StatePublic,
	}

	sexCol := &model.ColumnInfo{
		ID:     3,
		Name:   model.NewCIStr("SEX"),
		Offset: 2,
		FieldType: types.FieldType{
			Tp:      mysql.TypeEnum,
			Flag:    mysql.BinaryFlag,
			Flen:    -1,
			Decimal: -1,
			Charset: "binary",
			Collate: "binary",
			Elems:   []string{"male", "female"},
		},
		State: model.StatePublic,
	}

	t.Columns = []*model.ColumnInfo{userIDCol, userNameCol, sexCol}

	switch tt {
	case "hasID":
		userIDCol.Flag = mysql.NotNullFlag | mysql.PriKeyFlag | mysql.BinaryFlag | mysql.NoDefaultValueFlag

		t.PKIsHandle = true
		t.Indices = append(t.Indices, &model.IndexInfo{
			Primary: true,
			Columns: []*model.IndexColumn{{Name: userIDCol.Name}},
		})

	case "hasPK":
		userIDCol.Flag = mysql.NotNullFlag | mysql.PriKeyFlag | mysql.BinaryFlag | mysql.NoDefaultValueFlag | mysql.UniqueKeyFlag
		userNameCol.Flag = mysql.NotNullFlag | mysql.PriKeyFlag | mysql.NoDefaultValueFlag

		t.Indices = append(t.Indices, &model.IndexInfo{
			Primary: true,
			Unique:  true,
			Columns: []*model.IndexColumn{{Name: userIDCol.Name}, {Name: userNameCol.Name}},
		})
	}

	return t
}

func testGenRandomDatums(c *check.C, cols []*model.ColumnInfo) (datums []types.Datum) {
	for i := 0; i < len(cols); i++ {
		datum, _ := testGenDatum(c, cols[i], i)
		datums = append(datums, datum)
	}

	return
}

func testGenDeleteBinlog(c *check.C, t *model.TableInfo, r []types.Datum) []byte {
	var data []byte
	var err error

	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	colIDs := make([]int64, len(t.Columns))
	for i, col := range t.Columns {
		colIDs[i] = col.ID
	}
	data, err = tablecodec.EncodeOldRow(sc, r, colIDs, nil, nil)
	c.Assert(err, check.IsNil)
	return data
}

// generate raw row data by column.Type
func testGenDatum(c *check.C, col *model.ColumnInfo, base int) (types.Datum, interface{}) {
	var d types.Datum
	var e interface{}
	switch col.Tp {
	case mysql.TypeTiny, mysql.TypeInt24, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		if mysql.HasUnsignedFlag(col.Flag) {
			d.SetUint64(uint64(base))
			e = int64(base)
		} else {
			d.SetInt64(int64(base))
			e = int64(base)
		}
	case mysql.TypeFloat:
		d.SetFloat32(float32(base))
		e = float32(base)
	case mysql.TypeDouble:
		d.SetFloat64(float64(base))
		e = float64(base)
	case mysql.TypeNewDecimal:
		d.SetMysqlDecimal(types.NewDecFromInt(int64(base)))
		e = fmt.Sprintf("%v", base)
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		baseVal := "test"
		val := ""
		for i := 0; i < base; i++ {
			val = fmt.Sprintf("%s%s", val, baseVal)
		}
		d.SetString(val, "utf8mb4_bin")
		e = []byte(val)
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		baseVal := "test"
		val := ""
		for i := 0; i < base; i++ {
			val = fmt.Sprintf("%s%s", val, baseVal)
		}
		d.SetBytes([]byte(val))
		e = []byte(val)
	case mysql.TypeDuration:
		duration, err := types.ParseDuration(new(stmtctx.StatementContext), "10:10:10", 0)
		c.Assert(err, check.IsNil)
		d.SetMysqlDuration(duration)
		e = "10:10:10"
	case mysql.TypeDate, mysql.TypeNewDate:
		t := types.CurrentTime(mysql.TypeDate)
		d.SetMysqlTime(t)
		e = t.String()
	case mysql.TypeTimestamp:
		t := types.CurrentTime(mysql.TypeTimestamp)
		d.SetMysqlTime(t)
		e = t.String()
	case mysql.TypeDatetime:
		t := types.CurrentTime(mysql.TypeDatetime)
		d.SetMysqlTime(t)
		e = t.String()
	case mysql.TypeBit:
		bit, err := types.ParseBitStr("0b01")
		c.Assert(err, check.IsNil)
		d.SetMysqlBit(bit)
	case mysql.TypeSet:
		elems := []string{"a", "b", "c", "d"}
		set, err := types.ParseSetName(elems, elems[base-1])
		c.Assert(err, check.IsNil)
		d.SetMysqlSet(set, "utf8mb4_bin")
		e = set.Value
	case mysql.TypeEnum:
		elems := []string{"male", "female"}
		enum, err := types.ParseEnumName(elems, elems[base-1])
		c.Assert(err, check.IsNil)
		d.SetMysqlEnum(enum, "utf8mb4_bin")
		e = enum.Value
	}
	return d, e
}

func testGenInsertBinlog(c *check.C, t *model.TableInfo, r []types.Datum) []byte {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	var recordID int64 = 11

	colIDs := make([]int64, 0, len(r))
	row := make([]types.Datum, 0, len(r))
	for idx, col := range t.Columns {
		if testIsPKHandleColumn(t, col) {
			recordID = r[idx].GetInt64()
			continue
		}

		colIDs = append(colIDs, col.ID)
		row = append(row, r[idx])
	}

	value, err := tablecodec.EncodeOldRow(sc, row, colIDs, nil, nil)
	c.Assert(err, check.IsNil)

	handleVal, _ := codec.EncodeValue(sc, nil, types.NewIntDatum(recordID))
	bin := append(handleVal, value...)
	return bin
}

func testGenUpdateBinlog(c *check.C, t *model.TableInfo, oldData []types.Datum, newData []types.Datum) []byte {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	colIDs := make([]int64, 0, len(t.Columns))
	for _, col := range t.Columns {
		colIDs = append(colIDs, col.ID)
	}

	var bin []byte
	value, err := tablecodec.EncodeOldRow(sc, newData, colIDs, nil, nil)
	c.Assert(err, check.IsNil)
	oldValue, err := tablecodec.EncodeOldRow(sc, oldData, colIDs, nil, nil)
	c.Assert(err, check.IsNil)
	bin = append(oldValue, value...)
	return bin
}

func testIsPKHandleColumn(table *model.TableInfo, column *model.ColumnInfo) bool {
	return mysql.HasPriKeyFlag(column.Flag) && table.PKIsHandle
}
