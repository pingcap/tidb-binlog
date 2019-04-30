package translator

import (
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	parsermysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

// OpType represents type of the operation
type OpType byte

const (
	// DML is the constant OpType for delete operation
	DML = iota + 1
	// DDL is the constant OpType for ddl operation
	DDL
	// FLUSH is for wait all operation executed
	FLUSH
	// FAKE is for fake binlog
	FAKE
	// COMPLETE means the end of a binlog.
	COMPLETE
)

var providers = make(map[string]SQLTranslator)

// SQLTranslator is the interface for translating TiDB binlog to target sqls
type SQLTranslator interface {
	// Config set the configuration
	SetConfig(safeMode bool, sqlMode parsermysql.SQLMode)

	// GenInsertSQLs generates the insert sqls
	GenInsertSQLs(schema string, table *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, error)

	// GenUpdateSQLs generates the update sqls
	GenUpdateSQLs(schema string, table *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, bool, error)

	// GenDeleteSQLs generates the delete sqls by cols values
	GenDeleteSQLs(schema string, table *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, error)

	// GenDDLSQL generates the ddl sql by query string
	GenDDLSQL(sql string, schema string, commitTS int64) (string, error)
}

// Register registers the SQLTranslator into the providers
func Register(name string, provider SQLTranslator) {
	if provider == nil {
		log.Fatal("SQLTranslator: Register provide is nil")
	}

	if _, dup := providers[name]; dup {
		log.Fatal("SQLTranslator: Register called twice for provider " + name)
	}

	providers[name] = provider
}

// Unregister unregisters the SQLTranslator by name
func Unregister(name string) {
	delete(providers, name)
}

// New returns the SQLTranslator by given providerName
func New(providerName string) (SQLTranslator, error) {
	translator, ok := providers[providerName]
	if !ok {
		return nil, errors.Errorf("SQLTranslator: unknown provider %q", providerName)
	}

	return translator, nil
}

func insertRowToDatums(table *model.TableInfo, row []byte) (pk types.Datum, datums map[int64]types.Datum, err error) {
	colsTypeMap := util.ToColumnTypeMap(table.Columns)

	// decode the pk value
	var remain []byte
	remain, pk, err = codec.DecodeOne(row)
	if err != nil {
		return types.Datum{}, nil, errors.Trace(err)
	}

	datums, err = tablecodec.DecodeRow(remain, colsTypeMap, time.Local)
	if err != nil {
		return types.Datum{}, nil, errors.Trace(err)
	}

	// if only one column and IsPKHandleColumn then datums contains no any columns.
	if datums == nil {
		datums = make(map[int64]types.Datum)
	}

	for _, col := range table.Columns {
		if IsPKHandleColumn(table, col) {
			// If pk is handle, the datums TiDB write will always be Int64 type.
			// https://github.com/pingcap/tidb/blob/cd10bca6660937beb5d6de11d49ec50e149fe083/table/tables/tables.go#L721
			//
			// create table pk(id BIGINT UNSIGNED);
			// insert into pk(id) values(18446744073709551615)
			//
			// Will get -1 here, note: uint64(int64(-1)) = 18446744073709551615
			// so we change it to uint64 if the column type is unsigned
			datums[col.ID] = fixType(pk, col)
		}
	}

	return
}

func getDefaultOrZeroValue(col *model.ColumnInfo) types.Datum {
	// see https://github.com/pingcap/tidb/issues/9304
	// must use null if TiDB not write the column value when default value is null
	// and the value is null
	if !mysql.HasNotNullFlag(col.Flag) {
		return types.NewDatum(nil)
	}

	if col.GetDefaultValue() != nil {
		return types.NewDatum(col.GetDefaultValue())
	}

	if col.Tp == mysql.TypeEnum {
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		return types.NewDatum(col.FieldType.Elems[0])
	}

	return table.GetZeroValue(col)
}

func fixType(data types.Datum, col *model.ColumnInfo) types.Datum {
	if mysql.HasUnsignedFlag(col.Flag) {
		switch oldV := data.GetValue().(type) {
		case int64:
			log.Debugf("convert int64 type to uint64, value: %d", oldV)
			return types.NewDatum(uint64(oldV))
		}
	}
	return data
}
