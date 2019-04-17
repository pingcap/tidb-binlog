package translator

import "github.com/pingcap/parser/model"

// TableInfoGetter is used to get table info by table id of TiDB
type TableInfoGetter interface {
	TableByID(id int64) (info *model.TableInfo, ok bool)
	SchemaAndTableName(id int64) (string, string, bool)
}
