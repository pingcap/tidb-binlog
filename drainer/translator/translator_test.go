package translator

import (
	"fmt"
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

var _ = Suite(&testTranslatorSuite{})

type testTranslatorSuite struct{}

// test the already implemented translator, register and unregister function
func (t *testTranslatorSuite) TestRegisterAndUnregister(c *C) {
	hasTranslaters := []string{"mysql"}
	for _, name := range hasTranslaters {
		_, err := New(name)
		c.Assert(err, IsNil)
	}

	testName := "myTest"

	Register(testName, &mysqlTranslator{})
	_, err := New(testName)
	c.Assert(err, IsNil)

	Unregister(testName)
	_, err = New(testName)
	if err == nil {
		c.Fatalf("%s should not be existed", testName)
	}
}

// test all already implemented translator's translation function
func (t *testTranslatorSuite) TestTranslater(c *C) {
	s, err := New("mysql")
	c.Assert(err, IsNil)
	testGenInsertSQLs(c, s)
	testGenUpdateSQLs(c, s)
	testGenDeleteSQLs(c, s)
	testGenDDLSQL(c, s)
}

func testGenInsertSQLs(c *C, s SQLTranslator) {
	schema := "t"
	tables := []*model.TableInfo{testGenTable("normal"), testGenTable("hasPK"), testGenTable("hasID")}
	exceptedKeys := []int{0, 2, 1}
	for i, table := range tables {
		rowDatas, expected := testGenRowData(c, table.Columns, 1)
		binlog := testGenInsertBinlog(c, table, rowDatas)
		sqls, keys, vals, err := s.GenInsertSQLs(schema, table, [][]byte{binlog})
		c.Assert(keys[0], Equals, fmt.Sprintf("%v", expected[:exceptedKeys[i]]))
		c.Assert(err, IsNil)
		c.Assert(len(vals[0]), Equals, 3)
		c.Assert(sqls[0], Equals, "replace into `t`.`account` (`ID`,`NAME`,`SEX`) values (?,?,?);")
		for index := range vals {
			c.Assert(vals[0][index], DeepEquals, expected[index])
		}
	}

	rowDatas, _ := testGenRowData(c, tables[0].Columns, 1)
	binlog := testGenInsertBinlog(c, tables[0], rowDatas)
	_, _, _, err := s.GenInsertSQLs(schema, tables[0], [][]byte{binlog[6:]})
	if err == nil {
		c.Fatal("it's should panic")
	}
}

func testGenUpdateSQLs(c *C, s SQLTranslator) {
	schema := "t"
	tables := []*model.TableInfo{testGenTable("normal"), testGenTable("hasPK"), testGenTable("hasID")}
	exceptedSQL := []string{
		"update `t`.`account` set `ID` = ?, `NAME` = ?, `SEX` = ? where `ID` = ? and `NAME` = ? and `SEX` = ? limit 1;",
		"update `t`.`account` set `ID` = ?, `NAME` = ?, `SEX` = ? where `ID` = ? and `NAME` = ? limit 1;",
		"update `t`.`account` set `ID` = ?, `NAME` = ?, `SEX` = ? where `ID` = ? limit 1;",
	}
	exceptedNum := []int{6, 5, 4}
	exceptedKeys := []int{0, 2, 1}
	for index, t := range tables {
		oldRowDatas, whereExpected := testGenRowData(c, t.Columns, 1)
		newRowDatas, changeExpected := testGenRowData(c, t.Columns, 2)
		binlog := testGenUpdateBinlog(c, t, oldRowDatas, newRowDatas)
		sqls, keys, vals, err := s.GenUpdateSQLs(schema, t, [][]byte{binlog})
		c.Assert(keys[0], Equals, fmt.Sprintf("%v", whereExpected[:exceptedKeys[index]]))
		c.Assert(err, IsNil)
		c.Assert(len(vals[0]), Equals, exceptedNum[index])
		c.Assert(sqls[0], Equals, exceptedSQL[index])
		for index := range vals {
			if index < 3 {
				c.Assert(vals[0][index], DeepEquals, changeExpected[index])
				continue
			}
			c.Assert(vals[0][index], DeepEquals, whereExpected[index%3])
		}
	}

	rowDatas, _ := testGenRowData(c, tables[0].Columns, 1)
	binlog := testGenUpdateBinlog(c, tables[0], rowDatas, rowDatas)
	_, _, _, err := s.GenUpdateSQLs(schema, tables[0], [][]byte{binlog[6:]})
	if err == nil {
		c.Fatal("it's should panic")
	}
}

func testGenDeleteSQLs(c *C, s SQLTranslator) {
	schema := "t"
	tables := []*model.TableInfo{testGenTable("normal"), testGenTable("hasPK")}
	exceptedSQL := []string{
		"delete from `t`.`account` where `ID` = ? and `NAME` = ? and `SEX` = ? limit 1;",
		"delete from `t`.`account` where `ID` = ? and `NAME` = ? limit 1;",
	}
	exceptedNum := []int{3, 2}
	exceptedKeys := []int{0, 2}
	for index, t := range tables {
		rowDatas, expected := testGenRowData(c, t.Columns, 1)
		binlog := testGenDeleteBinlog(c, t, rowDatas)
		sqls, keys, vals, err := s.GenDeleteSQLs(schema, t, [][]byte{binlog})
		c.Assert(keys[0], Equals, fmt.Sprintf("%v", expected[:exceptedKeys[index]]))
		c.Assert(err, IsNil)
		c.Assert(len(vals[0]), Equals, exceptedNum[index])
		c.Assert(sqls[0], Equals, exceptedSQL[index])
		for index := range vals {
			c.Assert(vals[0][index], DeepEquals, expected[index])
		}
	}

	rowDatas, _ := testGenRowData(c, tables[0].Columns, 1)
	binlog := testGenDeleteBinlog(c, tables[0], rowDatas)
	_, _, _, err := s.GenDeleteSQLs(schema, tables[0], [][]byte{binlog[6:]})
	if err == nil {
		c.Fatal("it's should panic")
	}
}

func testGenDDLSQL(c *C, s SQLTranslator) {
	sql, err := s.GenDDLSQL("create database t", "t")
	c.Assert(err, IsNil)
	c.Assert(sql, Equals, "create database t;")

	sql, err = s.GenDDLSQL("drop table t", "t")
	c.Assert(err, IsNil)
	c.Assert(sql, Equals, "use `t`; drop table t;")
}

func testGenInsertBinlog(c *C, t *model.TableInfo, r []types.Datum) []byte {
	recordID := int64(11)
	for _, col := range t.Columns {
		if testIsPKHandleColumn(t, col) {
			recordID = r[col.Offset].GetInt64()
			break
		}
	}

	colIDs := make([]int64, 0, len(r))
	row := make([]types.Datum, 0, len(r))
	for _, col := range t.Columns {
		if testIsPKHandleColumn(t, col) {
			continue
		}
		colIDs = append(colIDs, col.ID)
		row = append(row, r[col.Offset])
	}

	value, err := tablecodec.EncodeRow(row, colIDs)
	c.Assert(err, IsNil)

	handleVal, _ := codec.EncodeValue(nil, types.NewIntDatum(recordID))
	bin := append(handleVal, value...)
	return bin
}

func testGenUpdateBinlog(c *C, t *model.TableInfo, oldData []types.Datum, newData []types.Datum) []byte {
	colIDs := make([]int64, 0, len(t.Columns))
	for _, col := range t.Columns {
		colIDs = append(colIDs, col.ID)
	}

	var bin []byte
	value, err := tablecodec.EncodeRow(newData, colIDs)
	c.Assert(err, IsNil)
	oldValue, err := tablecodec.EncodeRow(oldData, colIDs)
	c.Assert(err, IsNil)
	bin = append(oldValue, value...)
	return bin
}

func testGenDeleteBinlog(c *C, t *model.TableInfo, r []types.Datum) []byte {
	var data []byte
	var err error
	colIDs := make([]int64, len(t.Columns))
	for i, col := range t.Columns {
		colIDs[i] = col.ID
	}
	data, err = tablecodec.EncodeRow(r, colIDs)
	c.Assert(err, IsNil)
	return data
}

func testGenRowData(c *C, cols []*model.ColumnInfo, base int) ([]types.Datum, []interface{}) {
	datas := make([]types.Datum, 3)
	excepted := make([]interface{}, 3)
	for index, col := range cols {
		d, e := testGenDatum(c, col, base)
		datas[index] = d
		excepted[index] = e
	}
	return datas, excepted
}

// generate raw row data by column.Type
func testGenDatum(c *C, col *model.ColumnInfo, base int) (types.Datum, interface{}) {
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
		d.SetString(val)
		e = []byte(val)
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		baseVal := "test"
		val := ""
		for i := 0; i < base; i++ {
			val = fmt.Sprintf("%s%s", baseVal)
		}
		d.SetBytes([]byte(val))
		e = []byte(val)
	case mysql.TypeDuration:
		duration, err := types.ParseDuration("10:10:10", 0)
		c.Assert(err, IsNil)
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
		bit, err := types.ParseBit("0b01", 8)
		c.Assert(err, IsNil)
		d.SetMysqlBit(bit)
		e = bit.Value
	case mysql.TypeSet:
		elems := []string{"a", "b", "c", "d"}
		set, err := types.ParseSetName(elems, elems[base-1])
		c.Assert(err, IsNil)
		d.SetMysqlSet(set)
		e = set.Value
	case mysql.TypeEnum:
		elems := []string{"male", "female"}
		enum, err := types.ParseEnumName(elems, elems[base-1])
		c.Assert(err, IsNil)
		d.SetMysqlEnum(enum)
		e = enum.Value
	}
	return d, e
}

// hasID:  create table t(id int primary key, name varchar(45), sex enum("male", "female"));
// hasPK:  create table t(id int, name varchar(45), sex enum("male", "female"), PRIMARY KEY(id, name));
// normal: create table t(id int, name varchar(45), sex enum("male", "female"));
func testGenTable(tt string) *model.TableInfo {
	t := &model.TableInfo{}
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
	}

	t.Columns = []*model.ColumnInfo{userIDCol, userNameCol, sexCol}

	switch tt {
	case "hasPK":
		index := &model.IndexInfo{
			Primary: true,
			Columns: []*model.IndexColumn{{Name: userIDCol.Name, Offset: 0, Length: -1}, {Name: userNameCol.Name, Offset: 1, Length: -1}},
		}
		t.Indices = []*model.IndexInfo{index}
		userIDCol.Flag = mysql.NotNullFlag | mysql.PriKeyFlag | mysql.BinaryFlag | mysql.NoDefaultValueFlag

	case "hasID":
		t.PKIsHandle = true
		userIDCol.Flag = mysql.NotNullFlag | mysql.PriKeyFlag | mysql.BinaryFlag | mysql.NoDefaultValueFlag
		userNameCol.Flag = mysql.NotNullFlag | mysql.PriKeyFlag | mysql.NoDefaultValueFlag
	}

	return t
}

func testIsPKHandleColumn(table *model.TableInfo, column *model.ColumnInfo) bool {
	return mysql.HasPriKeyFlag(column.Flag) && table.PKIsHandle
}
