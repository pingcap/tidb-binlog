package translator

import (
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

// Hack: mark the column names for some types.
const dateColNamePrefix = "_tidb_date_"

func hackColumnNameAndType(colName string, colType *types.FieldType) (bool, string, string) {
	if colType.Tp == mysql.TypeDate || colType.Tp == mysql.TypeNewDate {
		return true, dateColNamePrefix + colName, "Int32"
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
