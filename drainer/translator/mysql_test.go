package translator

import (
	"testing"

	. "github.com/pingcap/check"

	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testDBSuite{})

type testDBSuite struct{}

func (s *testDBSuite) TestGenInsertSQLs(c *C) {
	ms := &mysqlTranslator{}
	schema := "t"
	table := generateTestTable()

	colIDs := make([]int64, 0, 3)
	row := make([]types.Datum, 0, 3)
	var enum mysql.Enum
	var err error

	for _, col := range table.Columns {
		if ms.isPKHandleColumn(table, col) {
			continue
		}
		colIDs = append(colIDs, col.ID)

		if col.ID == 2 {
			row = append(row, types.NewDatum("liming"))
		} else if col.ID == 3 {
			d := &types.Datum{}
			enum, err = mysql.ParseEnumName([]string{"female", "male"}, "male")
			c.Assert(err, IsNil)
			d.SetMysqlEnum(enum)
			row = append(row, *d)
		}
	}

	value, err := tablecodec.EncodeRow(row, colIDs)
	c.Assert(err, IsNil)
	handleVal, err := codec.EncodeValue(nil, types.NewIntDatum(1))
	c.Assert(err, IsNil)
	bin := append(handleVal, value...)

	sqls, vals, err := ms.GenInsertSQLs(schema, table, [][]byte{bin})
	c.Assert(err, IsNil)
	c.Assert(len(sqls), Equals, 1)
	if sqls[0] != "replace into t.account (id,name,male) values (?,?,?);" {
		c.Fatalf("insert sql %s , but want %s", sqls[0], "replace into t.account (id,name,male) values (?,?,?);")
	}

	c.Assert(len(vals[0]), Equals, 3)
	valID, ok := vals[0][0].(int64)
	c.Assert(ok, Equals, true)
	valName, ok := vals[0][1].([]byte)
	c.Assert(ok, Equals, true)
	valMale, ok := vals[0][2].(uint64)
	c.Assert(ok, Equals, true)

	if valID != 1 || string(valName) != "liming" || valMale != enum.Value {
		c.Fatalf("insert vals %v, but want  %d, %s, %d", vals[0], 1, []byte("liming"), enum.Value)
	}

	value, err = tablecodec.EncodeRow(row[0:len(row)-1], colIDs[0:len(colIDs)-1])
	c.Assert(err, IsNil)
	handleVal, err = codec.EncodeValue(nil, types.NewIntDatum(1))
	c.Assert(err, IsNil)
	bin = append(handleVal, value...)

	sqls, vals, err = ms.GenInsertSQLs(schema, table, [][]byte{bin})
	c.Assert(err, IsNil)
	c.Assert(len(sqls), Equals, 1)
	if sqls[0] != "replace into t.account (id,name,male) values (?,?,?);" {
		c.Fatalf("insert sql %s , but want %s", sqls[0], "replace into t.account (id,name,male) values (?,?,?);")
	}

	c.Assert(len(vals[0]), Equals, 3)
	valID, ok = vals[0][0].(int64)
	c.Assert(ok, Equals, true)
	valName, ok = vals[0][1].([]byte)
	c.Assert(ok, Equals, true)
	valMale, ok = vals[0][2].(uint64)
	c.Assert(ok, Equals, true)

	if valID != 1 || string(valName) != "liming" || valMale != 1 {
		c.Fatalf("insert vals %v, but want  %d, %s, %d", vals[0], 1, []byte("liming"), 1)
	}

	colIDs = append(colIDs, 4)
	row = append(row, types.NewIntDatum(8))
	value, err = tablecodec.EncodeRow(row, colIDs)
	c.Assert(err, IsNil)
	handleVal, err = codec.EncodeValue(nil, types.NewIntDatum(1))
	c.Assert(err, IsNil)
	bin = append(handleVal, value...)

	sqls, vals, err = ms.GenInsertSQLs(schema, table, [][]byte{bin})
	c.Assert(err, IsNil)
	c.Assert(len(sqls), Equals, 1)
	if sqls[0] != "replace into t.account (id,name,male) values (?,?,?);" {
		c.Fatalf("insert sql %s , but want %s", sqls[0], "replace into t.account (id,name,male) values (?,?,?);")
	}

	c.Assert(len(vals[0]), Equals, 3)
	valID, ok = vals[0][0].(int64)
	c.Assert(ok, Equals, true)
	valName, ok = vals[0][1].([]byte)
	c.Assert(ok, Equals, true)
	valMale, ok = vals[0][2].(uint64)
	c.Assert(ok, Equals, true)

	if valID != 1 || string(valName) != "liming" || valMale != enum.Value {
		c.Fatalf("insert vals %v, but want  %d, %s, %d", vals[0], 1, []byte("liming"), enum.Value)
	}
}

func (s *testDBSuite) TestGenUpdateSQLs(c *C) {
	ms := &mysqlTranslator{}
	schema := "t"
	table := generateNoPKTestTable()

	colIDs := make([]int64, 0, 3)
	row := make([]types.Datum, 0, 3)
	var enum mysql.Enum
	var err error

	for _, col := range table.Columns {
		colIDs = append(colIDs, col.ID)

		if col.ID == 1 {
			row = append(row, types.NewIntDatum(1))
		} else if col.ID == 2 {
			row = append(row, types.NewDatum("liming"))
		} else if col.ID == 3 {
			d := &types.Datum{}
			enum, err = mysql.ParseEnumName([]string{"female", "male"}, "male")
			c.Assert(err, IsNil)
			d.SetMysqlEnum(enum)
			row = append(row, *d)
		}
	}
	bin, err := tablecodec.EncodeRow(row, colIDs)
	c.Assert(err, IsNil)

	oldColIDs := make([]int64, 0, 3)
	oldRow := make([]types.Datum, 0, 3)

	for _, col := range table.Columns {
		oldColIDs = append(oldColIDs, col.ID)

		if col.ID == 1 {
			oldRow = append(oldRow, types.NewIntDatum(1))
		} else if col.ID == 2 {
			oldRow = append(oldRow, types.NewDatum("xiaoming"))
		} else if col.ID == 3 {
			d := &types.Datum{}
			enum, err = mysql.ParseEnumName([]string{"female", "male"}, "female")
			c.Assert(err, IsNil)
			d.SetMysqlEnum(enum)
			oldRow = append(oldRow, *d)
		}
	}

	oldBin, err := tablecodec.EncodeRow(oldRow, oldColIDs)
	c.Assert(err, IsNil)
	bin = append(bin, oldBin...)

	sqls, vals, err := ms.GenUpdateSQLs(schema, table, [][]byte{bin})
	c.Assert(err, IsNil)
	c.Assert(len(sqls), Equals, 1)
	if sqls[0] != "update t.account set ID = ?, Name = ?, male = ? where ID = ? and Name = ? and male = ? limit 1;" {
		c.Fatalf("update sql %s , but want %s", sqls[0], "update t.account set ID = ?, Name = ?, male = ? where ID = ? and Name = ? and male = ? limit 1;")
	}

	c.Assert(len(vals[0]), Equals, 6)
	valID, ok := vals[0][0].(int64)
	c.Assert(ok, Equals, true)
	valName, ok := vals[0][1].([]byte)
	c.Assert(ok, Equals, true)
	valMale, ok := vals[0][2].(uint64)
	c.Assert(ok, Equals, true)
	oldValID, ok := vals[0][3].(int64)
	c.Assert(ok, Equals, true)
	oldValName, ok := vals[0][4].([]byte)
	c.Assert(ok, Equals, true)
	oldValMale, ok := vals[0][5].(uint64)
	c.Assert(ok, Equals, true)

	if valID != 1 || string(valName) != "xiaoming" || valMale != 1 ||
		oldValID != 1 || string(oldValName) != "liming" || oldValMale != 2 {
		c.Fatalf("insert vals %v, but want  1, xiaoming, 1, 1,  liming, 2", vals[0])
	}

	table = generateTestTable()
	handleData, err := codec.EncodeValue(nil, types.NewIntDatum(1))
	bin = append(handleData, bin...)

	sqls, vals, err = ms.GenUpdateSQLs(schema, table, [][]byte{bin})
	c.Assert(err, IsNil)
	c.Assert(len(sqls), Equals, 1)
	if sqls[0] != "update t.account set ID = ?, Name = ?, male = ? where ID = ? limit 1;" {
		c.Fatalf("update sql %s , but want %s", sqls[0], "update t.account set ID = ?, Name = ?, male = ? where ID = ? limit 1;")
	}

	c.Assert(len(vals[0]), Equals, 4)
	valID, ok = vals[0][0].(int64)
	c.Assert(ok, Equals, true)
	valName, ok = vals[0][1].([]byte)
	c.Assert(ok, Equals, true)
	valMale, ok = vals[0][2].(uint64)
	c.Assert(ok, Equals, true)
	oldValID, ok = vals[0][3].(int64)
	c.Assert(ok, Equals, true)

	if valID != 1 || string(valName) != "xiaoming" || valMale != 1 || oldValID != 1 {
		c.Fatalf("insert vals %v, but want  1, xiaoming, 1", vals[0])
	}
}

func (s *testDBSuite) TestGenDeleteSQLs(c *C) {
	ms := &mysqlTranslator{}
	schema := "t"
	table := generateTestTable()

	rowsID := []int64{1}
	sqls, vals, err := ms.GenDeleteSQLsByID(schema, table, rowsID)
	c.Assert(err, IsNil)
	c.Assert(len(sqls), Equals, 1)
	if sqls[0] != "delete from t.account where ID = ? limit 1;" {
		c.Fatalf("delete sql %s, but want %s", sqls[0], "delete from t.account where ID = ? limit 1;")
	}

	c.Assert(len(vals[0]), Equals, 1)
	valID, ok := vals[0][0].(int64)
	c.Assert(ok, Equals, true)

	if valID != 1 {
		c.Fatalf("delete sql where %v, but want %v", vals[0], rowsID)
	}

	row := []types.Datum{types.NewDatum("liming")}
	bin, _ := codec.EncodeKey(nil, row...)

	sqls, vals, err = ms.GenDeleteSQLs(schema, table, DelByPK, [][]byte{bin})
	c.Assert(err, IsNil)
	c.Assert(len(sqls), Equals, 1)
	if sqls[0] != "delete from t.account where ID = ? limit 1;" {
		c.Fatalf("delete sql %s, but want %s", sqls[0], "delete from t.account where Name = ? limit 1;")
	}

	c.Assert(len(vals[0]), Equals, 1)
	valName, ok := vals[0][0].([]byte)
	c.Assert(ok, Equals, true)

	if string(valName) != "liming" {
		c.Fatalf("insert vals %s, but want  liming", valName)
	}

	colIDs := make([]int64, 0, 3)
	row = make([]types.Datum, 0, 3)
	var enum mysql.Enum

	for _, col := range table.Columns {
		colIDs = append(colIDs, col.ID)

		if col.ID == 1 {
			row = append(row, types.NewIntDatum(1))
		} else if col.ID == 2 {
			row = append(row, types.NewDatum("liming"))
		} else if col.ID == 3 {
			d := &types.Datum{}
			enum, err = mysql.ParseEnumName([]string{"female", "male"}, "male")
			c.Assert(err, IsNil)
			d.SetMysqlEnum(enum)
			row = append(row, *d)
		}
	}

	bin, err = tablecodec.EncodeRow(row, colIDs)
	c.Assert(err, IsNil)

	sqls, vals, err = ms.GenDeleteSQLs(schema, table, DelByCol, [][]byte{bin})
	c.Assert(err, IsNil)
	c.Assert(len(sqls), Equals, 1)
	if sqls[0] != "delete from t.account where ID = ? and Name = ? and male = ? limit 1;" {
		c.Fatalf("delete by col %s, want %s", sqls[0], "delete from t.account where ID = ? and Name = ? and male = ? limit 1;")
	}

	c.Assert(len(vals[0]), Equals, 3)
	valID, ok = vals[0][0].(int64)
	c.Assert(ok, Equals, true)
	valName, ok = vals[0][1].([]byte)
	c.Assert(ok, Equals, true)
	valMale, ok := vals[0][2].(uint64)
	c.Assert(ok, Equals, true)

	if valID != 1 || string(valName) != "liming" || valMale != enum.Value {
		c.Fatalf("insert vals %v, but want  %d, %s, %d", vals[0], 1, []byte("liming"), enum.Value)
	}
}

func (s *testDBSuite) TestGenDLLSQL(c *C) {
	ms := &mysqlTranslator{}

	sql, err := ms.GenDDLSQL("create database t", "t")
	c.Assert(err, IsNil)
	c.Assert(sql, Equals, "create database t;")

	sql, err = ms.GenDDLSQL("drop table t", "t")
	c.Assert(err, IsNil)
	c.Assert(sql, Equals, "use t; drop table t;")
}

func generateTestTable() *model.TableInfo {
	t := &model.TableInfo{}
	t.Name = model.NewCIStr("account")

	t.PKIsHandle = true
	userIDCol := &model.ColumnInfo{
		ID:     1,
		Name:   model.NewCIStr("ID"),
		Offset: 0,
	}
	userIDCol.Flag = 2
	idIndex := &model.IndexInfo{
		Primary: true,
		Columns: []*model.IndexColumn{&model.IndexColumn{Offset: 0}},
	}

	userNameCol := &model.ColumnInfo{
		ID:     2,
		Name:   model.NewCIStr("Name"),
		Offset: 1,
	}
	nameIndex := &model.IndexInfo{
		Primary: true,
		Columns: []*model.IndexColumn{&model.IndexColumn{Name: model.NewCIStr("Name"), Offset: 0}},
	}

	t.Indices = []*model.IndexInfo{nameIndex, idIndex}

	maleCol := &model.ColumnInfo{
		ID:           3,
		Name:         model.NewCIStr("male"),
		DefaultValue: uint64(1),
		Offset:       2,
	}

	t.Columns = []*model.ColumnInfo{userIDCol, userNameCol, maleCol}

	return t
}

func generateNoPKHandleTestTable() *model.TableInfo {
	t := &model.TableInfo{}
	t.Name = model.NewCIStr("account")

	t.PKIsHandle = false
	userIDCol := &model.ColumnInfo{
		ID:     1,
		Name:   model.NewCIStr("ID"),
		Offset: 0,
	}
	idIndex := &model.IndexInfo{
		Columns: []*model.IndexColumn{&model.IndexColumn{Offset: 0}},
	}

	userNameCol := &model.ColumnInfo{
		ID:     2,
		Name:   model.NewCIStr("Name"),
		Offset: 1,
	}
	nameIndex := &model.IndexInfo{
		Primary: true,
		Columns: []*model.IndexColumn{&model.IndexColumn{Name: model.NewCIStr("Name"), Offset: 0}},
	}

	t.Indices = []*model.IndexInfo{idIndex, nameIndex}

	maleCol := &model.ColumnInfo{
		ID:           3,
		Name:         model.NewCIStr("male"),
		DefaultValue: uint64(1),
		Offset:       2,
	}

	t.Columns = []*model.ColumnInfo{userIDCol, userNameCol, maleCol}

	return t
}

func generateNoPKTestTable() *model.TableInfo {
	t := &model.TableInfo{}
	t.Name = model.NewCIStr("account")

	t.PKIsHandle = true
	userIDCol := &model.ColumnInfo{
		ID:     1,
		Name:   model.NewCIStr("ID"),
		Offset: 0,
	}
	idIndex := &model.IndexInfo{
		Columns: []*model.IndexColumn{&model.IndexColumn{Offset: 0}},
	}

	userNameCol := &model.ColumnInfo{
		ID:     2,
		Name:   model.NewCIStr("Name"),
		Offset: 1,
	}
	nameIndex := &model.IndexInfo{
		Columns: []*model.IndexColumn{&model.IndexColumn{Name: model.NewCIStr("Name"), Offset: 0}},
	}

	t.Indices = []*model.IndexInfo{idIndex, nameIndex}

	maleCol := &model.ColumnInfo{
		ID:           3,
		Name:         model.NewCIStr("male"),
		DefaultValue: uint64(1),
		Offset:       2,
	}

	t.Columns = []*model.ColumnInfo{userIDCol, userNameCol, maleCol}

	return t
}
