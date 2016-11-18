package translator

import (
	"testing"
	"time"

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

var _ = Suite(&testTranslaterSuite{})

type testTranslaterSuite struct{}

// test the already implemented translater, register and unregister function
func (t *testTranslaterSuite) TestRegisterAndUnregister(c *C) {
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

// test all already implemented translater's translation function
func (t *testTranslaterSuite) TestTranslater(c *C) {
	for _, s := range providers {
		testGenInsertSQLs(c, s)
		testGenUpdateSQLs(c, s)
		testGenDeleteSQLs(c, s)
		testGenDeleteSQLsByID(c, s)
		testGenDDLSQL(c, s)
	}
}

func testGenInsertSQLs(c *C, s SQLTranslator) {
	schema := "t"
	tables := []*model.TableInfo{testGenTable("normal"), testGenTable("hasPK"), testGenTable("hasID")}
	for _, table := range tables {
		rowDatas, expected := testGenRowDatas(c, table.Columns)
		binlog := testGenInsertBinlog(c, table, rowDatas)
		sqls, vals, err := s.GenInsertSQLs(schema, table, [][]byte{binlog})
		c.Assert(err, IsNil)
		c.Assert(len(vals[0]), Equals, 3)
		c.Assert(sqls[0], Equals, "replace into t.account (id,name,sex) values (?,?,?);")
		for index := range vals {
			c.Assert(vals[0][index], DeepEquals, expected[index])
		}
	}

	rowDatas, _ := testGenRowDatas(c, tables[0].Columns)
	binlog := testGenInsertBinlog(c, tables[0], rowDatas)
	_, _, err := s.GenInsertSQLs(schema, tables[0], [][]byte{binlog[6:]})
	if err == nil {
		c.Fatal("it's should panic")
	}
}

func testGenUpdateSQLs(c *C, s SQLTranslator) {
	schema := "t"
	tables := []*model.TableInfo{testGenTable("normal"), testGenTable("hasPK"), testGenTable("hasID")}
	exceptedSqls := []string{"update t.account set ID = ?, NAME = ?, SEX = ? where ID = ? and NAME = ? and SEX = ? limit 1;",
		"update t.account set ID = ?, NAME = ?, SEX = ? where ID = ? and NAME = ? limit 1;",
		"update t.account set ID = ?, NAME = ?, SEX = ? where ID = ? limit 1;"}
	exceptedNums := []int{6, 5, 4}
	for index, t := range tables {
		rowDatas, expected := testGenRowDatas(c, t.Columns)
		binlog := testGenUpdateBinlog(c, t, rowDatas, rowDatas)
		sqls, vals, err := s.GenUpdateSQLs(schema, t, [][]byte{binlog})
		c.Assert(err, IsNil)
		c.Assert(len(vals[0]), Equals, exceptedNums[index])
		c.Assert(sqls[0], Equals, exceptedSqls[index])
		for index := range vals {
			c.Assert(vals[0][index], DeepEquals, expected[index%3])
		}
	}

	rowDatas, _ := testGenRowDatas(c, tables[0].Columns)
	binlog := testGenUpdateBinlog(c, tables[0], rowDatas, rowDatas)
	_, _, err := s.GenUpdateSQLs(schema, tables[0], [][]byte{binlog[6:]})
	if err == nil {
		c.Fatal("it's should panic")
	}
}

func testGenDeleteSQLs(c *C, s SQLTranslator) {
	schema := "t"
	tables := []*model.TableInfo{testGenTable("normal"), testGenTable("hasPK")}
	exceptedSqls := []string{"delete from t.account where ID = ? and NAME = ? and SEX = ? limit 1;",
		"delete from t.account where ID = ? and NAME = ? limit 1;"}
	exceptedNums := []int{3, 2}
	op := []OpType{DelByCol, DelByPK}
	for index, t := range tables {
		rowDatas, expected := testGenRowDatas(c, t.Columns)
		binlog := testGenDeleteBinlog(c, t, rowDatas)
		sqls, vals, err := s.GenDeleteSQLs(schema, t, op[index], [][]byte{binlog})
		c.Assert(err, IsNil)
		c.Assert(len(vals[0]), Equals, exceptedNums[index])
		c.Assert(sqls[0], Equals, exceptedSqls[index])
		for index := range vals {
			c.Assert(vals[0][index], DeepEquals, expected[index])
		}
	}

	rowDatas, _ := testGenRowDatas(c, tables[0].Columns)
	binlog := testGenDeleteBinlog(c, tables[0], rowDatas)
	_, _, err := s.GenDeleteSQLs(schema, tables[0], DelByCol, [][]byte{binlog[6:]})
	if err == nil {
		c.Fatal("it's should panic")
	}
}

func testGenDeleteSQLsByID(c *C, s SQLTranslator) {
	schema := "t"
	tables := []*model.TableInfo{testGenTable("hasID")}
	exceptedSqls := []string{"delete from t.account where ID = ? limit 1;"}
	exceptedNums := []int{1}
	for index, t := range tables {
		rowDatas, expected := testGenRowDatas(c, t.Columns)
		binlog := testGenDeleteBinlogByID(c, t, rowDatas)
		sqls, vals, err := s.GenDeleteSQLsByID(schema, t, []int64{binlog})
		c.Assert(err, IsNil)
		c.Assert(len(vals[0]), Equals, exceptedNums[index])
		c.Assert(sqls[0], Equals, exceptedSqls[index])
		for index := range vals {
			c.Assert(vals[0][index], DeepEquals, expected[index])
		}
	}
}

func testGenDDLSQL(c *C, s SQLTranslator) {
	sql, err := s.GenDDLSQL("create database t", "t")
	c.Assert(err, IsNil)
	c.Assert(sql, Equals, "create database t;")

	sql, err = s.GenDDLSQL("drop table t", "t")
	c.Assert(err, IsNil)
	c.Assert(sql, Equals, "use t; drop table t;")
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

	value, err := tablecodec.EncodeRow(newData, colIDs)
	c.Assert(err, IsNil)

	var h int64
	hasPK := false
	if t.PKIsHandle {
		for _, col := range t.Columns {
			if testIsPKHandleColumn(t, col) {
				hasPK = true
				h = oldData[col.Offset].GetInt64()
				break
			}
		}
	} else {
		for _, idx := range t.Indices {
			if idx.Primary {
				hasPK = true
				break
			}
		}
		h = 2
	}

	var bin []byte
	if hasPK {
		handleData, _ := codec.EncodeValue(nil, types.NewIntDatum(h))
		bin = append(handleData, value...)
	} else {
		oldData, err := tablecodec.EncodeRow(oldData, colIDs)
		c.Assert(err, IsNil)
		bin = append(oldData, value...)
	}

	return bin
}

func testGenDeleteBinlogByID(c *C, t *model.TableInfo, r []types.Datum) int64 {
	var h int64
	var hasPK bool
	if t.PKIsHandle {
		for _, col := range t.Columns {
			if testIsPKHandleColumn(t, col) {
				hasPK = true
				h = r[col.Offset].GetInt64()
				break
			}
		}

		if !hasPK {
			c.Fatal("this case don't have primary id")
		}
		return h
	}

	c.Fatal("this case don't have primary id")
	return 0
}

func testGenDeleteBinlog(c *C, t *model.TableInfo, r []types.Datum) []byte {
	var primaryIdx *model.IndexInfo
	for _, idx := range t.Indices {
		if idx.Primary {
			primaryIdx = idx
			break
		}
	}
	var data []byte
	var err error
	if primaryIdx != nil {
		indexedValues := make([]types.Datum, len(primaryIdx.Columns))
		for i := range indexedValues {
			indexedValues[i] = r[primaryIdx.Columns[i].Offset]
		}
		data, err = codec.EncodeKey(nil, indexedValues...)
		c.Assert(err, IsNil)
		return data
	}
	colIDs := make([]int64, len(t.Columns))
	for i, col := range t.Columns {
		colIDs[i] = col.ID
	}
	data, err = tablecodec.EncodeRow(r, colIDs)
	c.Assert(err, IsNil)
	return data
}

func testGenRowDatas(c *C, cols []*model.ColumnInfo) ([]types.Datum, []interface{}) {
	datas := make([]types.Datum, 3)
	excepted := make([]interface{}, 3)
	for index, col := range cols {
		d, e := testGenDatum(c, col)
		datas[index] = d
		excepted[index] = e
	}
	return datas, excepted
}

// generate raw row data by column.Type
func testGenDatum(c *C, col *model.ColumnInfo) (types.Datum, interface{}) {
	var d types.Datum
	var e interface{}
	switch col.Tp {
	case mysql.TypeTiny, mysql.TypeInt24, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		if mysql.HasUnsignedFlag(col.Flag) {
			d.SetUint64(1)
			e = int64(1)
		} else {
			d.SetInt64(1)
			e = int64(1)
		}
	case mysql.TypeFloat:
		d.SetFloat32(1)
		e = 1.0
	case mysql.TypeDouble:
		d.SetFloat64(1)
		e = 1.0
	case mysql.TypeNewDecimal:
		d.SetMysqlDecimal(mysql.NewDecFromInt(1))
		e = 1.0
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		d.SetString("test")
		e = []byte("test")
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		d.SetBytes([]byte("test"))
		e = []byte("test")
	case mysql.TypeDuration:
		duration, err := mysql.ParseDuration("10:10:10", 0)
		c.Assert(err, IsNil)
		d.SetMysqlDuration(duration)
		e = "10:10:10"
	case mysql.TypeDate, mysql.TypeNewDate:
		t := mysql.Time{Time: time.Now(), Type: mysql.TypeDate, Fsp: 0}
		d.SetMysqlTime(t)
		e = t.String()
	case mysql.TypeTimestamp:
		t := mysql.Time{Time: time.Now(), Type: mysql.TypeTimestamp, Fsp: 0}
		d.SetMysqlTime(t)
		e = t.String()
	case mysql.TypeDatetime:
		t := mysql.Time{Time: time.Now(), Type: mysql.TypeDatetime, Fsp: 0}
		d.SetMysqlTime(t)
		e = t.String()
	case mysql.TypeBit:
		bit, err := mysql.ParseBit("0b01", 8)
		c.Assert(err, IsNil)
		d.SetMysqlBit(bit)
		e = bit.Value
	case mysql.TypeSet:
		elems := []string{"a", "b", "c", "d"}
		set, err := mysql.ParseSetName(elems, "a")
		c.Assert(err, IsNil)
		d.SetMysqlSet(set)
		e = set.Value
	case mysql.TypeEnum:
		elems := []string{"male", "female"}
		enum, err := mysql.ParseEnumName(elems, "male")
		c.Assert(err, IsNil)
		d.SetMysqlEnum(enum)
		e = enum.Value
	}
	return d, e
}

// hasID: create table t(id int primary key, name varchar(45), sex enum("male", "female"));
// hasPK: create table t(id int, name varchar(45), sex enum("male", "female"), PRIMARY KEY(id, name));
// normal: reate table t(id int, name varchar(45), sex enum("male", "female"));
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
