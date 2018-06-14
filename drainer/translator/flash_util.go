package translator

import (
	"fmt"
	"math"
	gotime "time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

const implicitColName = "_tidb_rowid"
const internalVersionColName = "_INTERNAL_VERSION"
const internalDelmarkColName = "_INTERNAL_DELMARK"
const emptySQL = "select 1"

func genEmptySQL(reason string) string {
	return emptySQL + " -- Reason: " + reason
}

func fakeImplicitColumn(table *model.TableInfo) *model.ColumnInfo {
	for _, col := range table.Columns {
		// since we appended a fake key, remove the original keys
		if mysql.HasPriKeyFlag(col.Flag) {
			col.Flag ^= mysql.PriKeyFlag
		}
	}

	handleColumn := model.NewExtraHandleColInfo()
	// Transform TiDB's default extra handle column name and type into our own.
	handleColumn.Name = model.NewCIStr(implicitColName)
	handleColumn.Tp = mysql.TypeLonglong
	table.Columns = append(table.Columns, handleColumn)

	table.PKIsHandle = true
	return handleColumn
}

func isPrimaryKeyColumn(colDef *ast.ColumnDef) bool {
	for _, option := range colDef.Options {
		if option.Tp == ast.ColumnOptionPrimaryKey &&
			isHandleTypeColumn(colDef) {
			return true
		}
	}
	return false
}

func pkHandleColumn(table *model.TableInfo) *model.ColumnInfo {
	for _, col := range table.Columns {
		if isPKHandleColumn(table, col) {
			return col
		}
	}

	return nil
}

func pkIndexColumns(table *model.TableInfo) ([]*model.ColumnInfo, error) {
	col := pkHandleColumn(table)
	if col != nil {
		return []*model.ColumnInfo{col}, nil
	}

	var cols []*model.ColumnInfo
	for _, idx := range table.Indices {
		if idx.Primary {
			columns := make(map[string]*model.ColumnInfo)

			for _, col := range table.Columns {
				columns[col.Name.O] = col
			}

			for _, col := range idx.Columns {
				if column, ok := columns[col.Name.O]; ok {
					cols = append(cols, column)
				}
			}

			if len(cols) == 0 {
				return nil, errors.New("primay index is empty, but should not be empty")
			}

			return cols, nil
		}
	}

	return cols, nil
}

func isNullable(colDef *ast.ColumnDef) bool {
	if isPrimaryKeyColumn(colDef) {
		return false
	}
	for _, option := range colDef.Options {
		if option.Tp == ast.ColumnOptionNotNull {
			return false
		}
	}
	return true
}

func isHandleTypeColumn(colDef *ast.ColumnDef) bool {
	tp := colDef.Tp.Tp
	return tp == mysql.TypeTiny ||
		tp == mysql.TypeShort ||
		tp == mysql.TypeInt24 ||
		tp == mysql.TypeLong ||
		tp == mysql.TypeLonglong
}

func toFlashColumnTypeMap(columns []*model.ColumnInfo) map[int64]*types.FieldType {
	colTypeMap := make(map[int64]*types.FieldType)
	for _, col := range columns {
		colTypeMap[col.ID] = &col.FieldType
	}

	return colTypeMap
}

func makeRow(pk int64, values []interface{}, version uint64, delFlag uint8) []interface{} {
	var row []interface{}
	row = append(row, pk)
	row = append(row, values...)
	row = append(row, version)
	row = append(row, delFlag)
	return row
}

func makeInternalVersionValue(ver uint64) uint64 {
	return ver
}

func makeInternalDelmarkValue(del bool) uint8 {
	if del {
		return uint8(1)
	}
	return uint8(0)
}

func decodeFlashOldAndNewRow(b []byte, cols map[int64]*types.FieldType, loc *gotime.Location) (map[int64]types.Datum, map[int64]types.Datum, error) {
	if b == nil {
		return nil, nil, nil
	}
	if b[0] == codec.NilFlag {
		return nil, nil, nil
	}

	cnt := 0
	var (
		data   []byte
		err    error
		oldRow = make(map[int64]types.Datum, len(cols))
		newRow = make(map[int64]types.Datum, len(cols))
	)
	for len(b) > 0 {
		// Get col id.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		_, cid, err := codec.DecodeOne(data)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		// Get col value.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		id := cid.GetInt64()
		ft, ok := cols[id]
		if ok {
			v, err := tablecodec.DecodeColumnValue(data, ft, loc)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}

			if _, ok := oldRow[id]; ok {
				newRow[id] = v
			} else {
				oldRow[id] = v
			}

			cnt++
			if cnt == len(cols)*2 {
				// Get enough data.
				break
			}
		}
	}

	if cnt != len(cols)*2 || len(newRow) != len(oldRow) {
		return nil, nil, errors.Errorf(" row data is corrupted %v", b)
	}

	return oldRow, newRow, nil
}

// Convert datum to CH raw data, data type must be strictly matching the rules in analyzeColumnDef.
func formatFlashData(data types.Datum, ft types.FieldType) (interface{}, error) {
	if data.GetValue() == nil {
		return nil, nil
	}

	switch ft.Tp {
	case mysql.TypeBit: // UInt64
		ui, err := data.GetMysqlBit().ToInt()
		if err != nil {
			return data, errors.Trace(err)
		}
		return ui, nil
	case mysql.TypeTiny: // UInt8/Int8
		if mysql.HasUnsignedFlag(ft.Flag) {
			return uint8(data.GetInt64()), nil
		}
		return int8(data.GetInt64()), nil
	case mysql.TypeShort: // UInt16/Int16
		if mysql.HasUnsignedFlag(ft.Flag) {
			return uint16(data.GetInt64()), nil
		}
		return int16(data.GetInt64()), nil
	case mysql.TypeYear: // Int16
		return int16(data.GetInt64()), nil
	case mysql.TypeLong, mysql.TypeInt24: // UInt32/Int32
		if mysql.HasUnsignedFlag(ft.Flag) {
			return uint32(data.GetInt64()), nil
		}
		return int32(data.GetInt64()), nil
	case mysql.TypeFloat: // Float32
		return data.GetFloat32(), nil
	case mysql.TypeDouble: // Float64
		return data.GetFloat64(), nil
	case mysql.TypeNewDecimal, mysql.TypeDecimal: // Float64
		// TODO: map decimal to CH decimal.
		f, err := data.GetMysqlDecimal().ToFloat64()
		if err != nil {
			log.Warnf("Corrupted decimal data: %v, will leave it zero.", data.GetMysqlDecimal())
			f = 0
		}
		return f, nil
	case mysql.TypeDate, mysql.TypeNewDate, mysql.TypeDatetime, mysql.TypeTimestamp: // Int64
		var unix = int64(0)
		// TiDB won't accept invalid date/time EXCEPT "0000-00-00", which is default value for not-null columns. So leave it zero.
		if mysqlTime := data.GetMysqlTime(); !mysqlTime.IsZero() {
			// Calculate the go time.
			time := mysqlTime.Time
			// Using UTC timezone
			timezone := gotime.UTC
			// Need to consider timezone for DateTime and Timestamp, which are mapped to timezone-sensitive DateTime in CH.
			if ft.Tp == mysql.TypeDatetime || ft.Tp == mysql.TypeTimestamp {
				timezone = gotime.Local
			}
			goTime := gotime.Date(time.Year(), gotime.Month(time.Month()), time.Day(), time.Hour(), time.Minute(), time.Second(), time.Microsecond()*1000, timezone)
			unix = goTime.Unix()
			// Zero the negative unix time to prevent overflow in CH.
			if unix < 0 {
				log.Warnf("Date/DateTime data before 1970-01-01 UTC: %v, will leave it zero.", mysqlTime.String())
				unix = 0
			}
		}
		return unix, nil
	case mysql.TypeDuration: // Int64
		num, err := data.GetMysqlDuration().ToNumber().ToInt()
		if err != nil {
			log.Warnf("Corrupted Duration data: %v, will leave it zero.", data.GetMysqlDuration())
			num = 0
		}
		return num, nil
	case mysql.TypeLonglong: // UInt64/Int64
		if mysql.HasUnsignedFlag(ft.Flag) {
			return data.GetUint64(), nil
		}
		return data.GetInt64(), nil
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString: // String
		return data.GetString(), nil
	case mysql.TypeEnum: // UInt16
		return int16(data.GetMysqlEnum().Value), nil
	case mysql.TypeSet: // String
		return data.GetMysqlSet().String(), nil
	case mysql.TypeJSON: // String
		return data.GetMysqlJSON().String(), nil
	case mysql.TypeGeometry:
		// TiDB doesn't have Geometry type, so put it null.
		return nil, nil
	}

	return nil, nil
}

// Poor man's expression eval function, that is mostly used for DDL that refers constant expressions,
// such as default value in CREATE/ALTER TABLE.
// Support very limited expression types: ValueExpr/Function(current_timestamp)/UnaryOp(-)
func formatFlashLiteral(expr ast.ExprNode, ft *types.FieldType) (string, bool, error) {
	shouldQuote := false
	switch ft.Tp {
	case mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString, mysql.TypeSet:
		shouldQuote = true
	}
	switch e := expr.(type) {
	case *ast.ValueExpr:
		value := *e.GetDatum()
		if value.GetValue() == nil {
			return "NULL", false, nil
		}
		switch ft.Tp {
		case mysql.TypeNull:
			return "NULL", false, nil
			// case mysql.TypeJSON, mysql.TypeGeometry:
			// TiDB doesn't allow default value for JSON types, and doesn't have Geometry at all.
		default:
			// Do conversion.
			converted, err := convertValueType(value, expr.GetType(), ft)
			if err != nil {
				return "", false, errors.Trace(err)
			}
			return fmt.Sprintf("%v", converted), shouldQuote, nil
		}
	case *ast.FuncCallExpr:
		// Evaluate current time immediately as CH won't use the instant value for now() and gives different values for each time data is retrieved.
		if e.FnName.L == ast.CurrentTimestamp {
			t := types.NewTimeDatum(types.CurrentTime(e.GetType().Tp))
			return fmt.Sprintf("'%v'", t.GetMysqlTime().String()), shouldQuote, nil
		}
		return "", false, errors.New(fmt.Sprintf("Function expression %s is not supported.", e.FnName))
	case *ast.UnaryOperationExpr:
		op := ""
		switch e.Op {
		case opcode.Minus:
			if ft.Tp != mysql.TypeYear {
				// Year will ignore the heading -.
				op = "-"
			}
		case opcode.Plus:
			if ft.Tp != mysql.TypeYear {
				// Year will ignore the heading +.
				op = "+"
			}
		default:
			return "", false, errors.New(fmt.Sprintf("Op %s is not supported.", e.Op.String()))
		}
		child, _, err := formatFlashLiteral(e.V, ft)
		if err != nil {
			return "", false, errors.Trace(err)
		}
		return fmt.Sprintf("%s%s", op, child), shouldQuote, nil
	default:
		return "", false, errors.New(fmt.Sprintf("Expression %v is not supported.", e))
	}
}

// Poor man's data conversion function.
// Support very limited conversions, such as among numeric/string/date/time.
func convertValueType(data types.Datum, source *types.FieldType, target *types.FieldType) (interface{}, error) {
	switch target.Tp {
	case mysql.TypeSet:
		var set types.Set
		var err error
		switch source.Tp {
		case mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString:
			set, err = types.ParseSetName(target.Elems, data.GetString())
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
			set, err = types.ParseSetValue(target.Elems, data.GetUint64())
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		return escapeString(set.Name), nil
	case mysql.TypeEnum:
		switch source.Tp {
		case mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString:
			return fmt.Sprintf("'%s'", escapeString(data.GetString())), nil
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
			return data.GetInt64(), nil
		}
	case mysql.TypeDate, mysql.TypeNewDate:
		// Towards date types, either 'YYYY-MM-DD hh:mm:ss' or 'YYYY-MM-DD' is OK.
		var mysqlTime types.Time
		var err error
		switch source.Tp {
		case mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString:
			mysqlTime, err = types.ParseTime(&stmtctx.StatementContext{TimeZone: gotime.UTC}, data.GetString(), target.Tp, 0)
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
			mysqlTime, err = types.ParseTimeFromInt64(&stmtctx.StatementContext{TimeZone: gotime.UTC}, data.GetInt64())
		case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
			mysqlTime, err = types.ParseTimeFromFloatString(&stmtctx.StatementContext{TimeZone: gotime.UTC}, data.GetMysqlDecimal().String(), target.Tp, 0)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		return fmt.Sprintf("'%v'", mysqlTime), nil
	case mysql.TypeTimestamp, mysql.TypeDatetime:
		// Towards time types, convert to string formatted as 'YYYY-MM-DD hh:mm:ss'
		var mysqlTime types.Time
		var err error
		switch source.Tp {
		case mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString:
			mysqlTime, err = types.ParseTime(&stmtctx.StatementContext{TimeZone: gotime.Local}, data.GetString(), target.Tp, 0)
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
			mysqlTime, err = types.ParseTimeFromInt64(&stmtctx.StatementContext{TimeZone: gotime.Local}, data.GetInt64())
		case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
			mysqlTime, err = types.ParseTimeFromFloatString(&stmtctx.StatementContext{TimeZone: gotime.Local}, data.GetMysqlDecimal().String(), target.Tp, 0)
		}
		formatted, err := mysqlTime.DateFormat("%Y-%m-%d %H:%i:%S")
		if err != nil {
			return nil, errors.Trace(err)
		}
		return fmt.Sprintf("'%s'", formatted), nil
	case mysql.TypeDuration:
		// Towards duration type, convert to gotime.Duration.
		var duration types.Duration
		var err error
		switch source.Tp {
		case mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString:
			duration, err = types.ParseDuration(data.GetString(), 0)
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
			duration, err = types.ParseDuration(fmt.Sprintf("%v", data.GetInt64()), 0)
		case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
			duration, err = types.ParseDuration(data.GetMysqlDecimal().String(), 0)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		return duration.ToNumber().ToInt()
	case mysql.TypeYear:
		var year int16
		var err error
		switch source.Tp {
		case mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString:
			// As TiDB allows string literal like '1998.0' to be year value, and ParseYear() will error out for it, we need to cast to integer by ourselves.
			var d float64
			_, err := fmt.Sscanf(data.GetString(), "%f", &d)
			if err != nil {
				return nil, errors.Trace(err)
			}
			year, err = types.ParseYear(fmt.Sprintf("%d", int64(math.Abs(d))))
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
			year, err = types.ParseYear(fmt.Sprintf("%v", data.GetInt64()))
		case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
			d, err := data.GetMysqlDecimal().ToFloat64()
			if err != nil {
				return nil, errors.Trace(err)
			}
			year, err = types.ParseYear(fmt.Sprintf("%d", int64(math.Abs(d))))
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		return year, nil
	case mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString:
		// Towards string types, escape it. Will single-quote in upper logic.
		s := ""
		switch source.Tp {
		case mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString:
			s = data.GetString()
		case mysql.TypeDecimal, mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
			s = fmt.Sprintf("%v", data.GetMysqlDecimal())
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
			s = fmt.Sprintf("%v", data.GetValue())
		}
		return escapeString(s), nil
	case mysql.TypeBit:
		// Towards bit, convert to raw bytes and return as uint64.
		switch source.Tp {
		case mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString:
			return data.GetMysqlBit().ToInt()
		case mysql.TypeFloat, mysql.TypeDecimal, mysql.TypeDouble, mysql.TypeNewDecimal:
			// TiDB rounds float to uint for bit.
			f, err := data.GetMysqlDecimal().ToFloat64()
			if err != nil {
				return nil, errors.Trace(err)
			}
			f = types.RoundFloat(f)
			return uint64(int64(f)), nil
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
			return data.GetValue(), nil
		}
	case mysql.TypeFloat, mysql.TypeDecimal, mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeDouble, mysql.TypeLonglong, mysql.TypeInt24, mysql.TypeNewDecimal:
		// Towards numeric types, do really conversion.
		switch source.Tp {
		case mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString:
			return data.GetString(), nil
		case mysql.TypeDecimal, mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
			return data.GetMysqlDecimal(), nil
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
			return data.GetValue(), nil
		}
	}
	return nil, errors.Errorf("Unable to convert data %v from type %s to type %s.", data, source.String(), target.String())
}

// Escape a string to CH string literal.
// See: http://clickhouse-docs.readthedocs.io/en/latest/query_language/syntax.html
func escapeString(s string) string {
	escaped := ""
	for _, c := range s {
		switch c {
		case '\\':
			escaped += "\\\\"
		case '\'':
			escaped += "\\'"
		case '\b':
			escaped += "\\b"
		case '\f':
			escaped += "\\f"
		case '\r':
			escaped += "\\r"
		case '\n':
			escaped += "\\n"
		case '\t':
			escaped += "\\t"
		case 0:
			escaped += "\\0"
		default:
			escaped += string(c)
		}
	}
	return escaped
}
