package translator

import (
	"fmt"

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

// Hack: mark the column names for some types.
const dateColNamePrefix = "_tidb_date_"
const decimalColNamePrefix = "_tidb_decimal_"

func hackColumnNameAndType(colName string, colType *types.FieldType) (bool, string, string) {
	if colType.Tp == mysql.TypeDate || colType.Tp == mysql.TypeNewDate {
		return true, dateColNamePrefix + colName, "Int32"
	} else if (colType.Tp == mysql.TypeDecimal || colType.Tp == mysql.TypeNewDecimal) && colType.Decimal == 0 {
		return true, decimalColNamePrefix + colName, "String"
	}
	return false, "", ""
}

// Hack: store Date using Int32 as days since epoch, CH driver accepts int64 for Int32 type, so leave it int64.
func hackFormatDateData(unix int64, ft *types.FieldType) (bool, int64) {
	if ft.Tp == mysql.TypeDate || ft.Tp == mysql.TypeNewDate {
		return true, unix / 24 / 3600
	}
	return false, -1
}

// Hack: store decimal using String.
func hackFormatDecimalData(data *types.Datum, ft *types.FieldType) (bool, string) {
	if (ft.Tp == mysql.TypeDecimal || ft.Tp == mysql.TypeNewDecimal) && ft.Decimal == 0 {
		return true, data.GetMysqlDecimal().String()
	}
	return false, ""
}

// Hack: store decimal using String.
func hackConvertDecimalType(data types.Datum, source *types.FieldType, target *types.FieldType) (bool, string) {
	if target.Tp != mysql.TypeDecimal && target.Tp != mysql.TypeNewDecimal || target.Decimal != 0 {
		return false, ""
	}
	switch source.Tp {
	case mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString:
		return true, data.GetString()
	case mysql.TypeDecimal, mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
		return true, data.GetMysqlDecimal().String()
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
		return true, fmt.Sprintf("%v", data.GetValue())
	}
	return false, ""
}

func hackShouldQuote(ft *types.FieldType) bool {
	if (ft.Tp == mysql.TypeDecimal || ft.Tp == mysql.TypeNewDecimal) && ft.Decimal == 0 {
		return true
	}
	return false
}
