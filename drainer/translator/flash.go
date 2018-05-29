package translator

import (
	"fmt"
	"strconv"
	"strings"
	gotime "time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/dml"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"math"
)

const implicitColName = "_tidb_rowid"
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

	handleColumn := &model.ColumnInfo{
		ID:   implicitColID,
		Name: model.NewCIStr(implicitColName),
	}
	handleColumn.Flag = mysql.PriKeyFlag
	handleColumn.Tp = mysql.TypeLonglong
	table.Columns = append(table.Columns, handleColumn)

	table.PKIsHandle = true
	return handleColumn
}

func extractCreateTable(stmt *ast.CreateTableStmt, schema string) (string, error) {
	// extract primary key
	pkColumn, explicitHandle := extractRowHandle(stmt)
	// var buffer bytes.Buffer
	tableName := stmt.Table.Name.L
	colStrs := make([]string, len(stmt.Cols))
	for i, colDef := range stmt.Cols {
		colStr, _ := analyzeColumnDef(colDef, pkColumn)
		colStrs[i] = colStr
	}
	if !explicitHandle {
		colStr := fmt.Sprintf("`%s` %s", pkColumn, "Int64")
		colStrs = append([]string{colStr}, colStrs...)
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s) ENGINE MutableMergeTree((`%s`), 8192);", schema, tableName, strings.Join(colStrs, ","), pkColumn), nil
}

func extractAlterTable(stmt *ast.AlterTableStmt, schema string) (string, error) {
	if stmt.Specs[0].Tp == ast.AlterTableRenameTable {
		return makeRenameTableStmt(schema, stmt.Table, stmt.Specs[0].NewTable), nil
	}
	specStrs := make([]string, len(stmt.Specs))
	for i, spec := range stmt.Specs {
		specStr, err := analyzeAlterSpec(spec)
		if err != nil {
			return "", errors.Trace(err)
		}
		specStrs[i] = specStr
	}

	tableName := stmt.Table.Name.L
	return fmt.Sprintf("ALTER TABLE `%s`.`%s` %s;", schema, tableName, strings.Join(specStrs, ", ")), nil
}

func extractRenameTable(stmt *ast.RenameTableStmt, schema string) (string, error) {
	return makeRenameTableStmt(schema, stmt.OldTable, stmt.NewTable), nil
}

func makeRenameTableStmt(schema string, table *ast.TableName, newTable *ast.TableName) string {
	tableName := table.Name.L
	var newSchema = schema
	if len(newTable.Schema.String()) > 0 {
		newSchema = newTable.Schema.L
	}
	newTableName := newTable.Name.L
	return fmt.Sprintf("RENAME TABLE `%s`.`%s` TO `%s`.`%s`;", schema, tableName, newSchema, newTableName)
}

func extractDropTable(stmt *ast.DropTableStmt, schema string) (string, error) {
	// TODO: Make drop multiple tables works
	tableName := stmt.Tables[0].Name.L
	return fmt.Sprintf("DROP TABLE `%s`.`%s`;", schema, tableName), nil
}

func extractCreateDatabase(stmt *ast.CreateDatabaseStmt) (string, error) {
	dbName := strings.ToLower(stmt.Name)
	return fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", dbName), nil
}

func extractDropDatabase(stmt *ast.DropDatabaseStmt) (string, error) {
	dbName := strings.ToLower(stmt.Name)
	// http://clickhouse-docs.readthedocs.io/en/latest/query_language/queries.html#drop
	// Drop cascade semantics and should be save to not consider sequence
	return fmt.Sprintf("DROP DATABASE `%s`;", dbName), nil
}

// extract single row handle column, if implicit, generate one
func extractRowHandle(stmt *ast.CreateTableStmt) (colName string, explicitHandle bool) {
	constrains := stmt.Constraints
	columns := stmt.Cols
	var primaryCnt = 0
	var primaryColumn = ""
	for _, colDef := range columns {
		cNameLowercase := colDef.Name.Name.L
		if isPrimaryKeyColumn(colDef) {
			primaryCnt += 1
			primaryColumn = cNameLowercase
		} else {
			for _, constrain := range constrains {
				// row handle only applies when single integer key
				if len(constrain.Keys) != 1 {
					continue
				}
				if constrain.Tp == ast.ConstraintPrimaryKey &&
					isHandleTypeColumn(colDef) &&
					cNameLowercase == constrain.Keys[0].Column.Name.L {
					return cNameLowercase, true
				}
			}
		}
	}

	if primaryCnt == 1 {
		return primaryColumn, true
	}
	// no explicit handle column, generate one
	return implicitColName, false
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

func analyzeAlterSpec(alterSpec *ast.AlterTableSpec) (string, error) {
	switch alterSpec.Tp {
	case ast.AlterTableOption:
		return genEmptySQL(strconv.Itoa(int(alterSpec.Tp))), nil
	case ast.AlterTableAddColumns:
		var colDefStr = ""
		var colPosStr = ""
		var err error = nil
		// TODO: Support add multiple columns.
		colDefStr, err = analyzeColumnDef(alterSpec.NewColumns[0], "")
		if err != nil {
			return "", errors.Trace(err)
		}
		if alterSpec.Position != nil && alterSpec.Position.Tp != ast.ColumnPositionNone {
			colPosStr, err = analyzeColumnPosition(alterSpec.Position)
			if err != nil {
				return "", errors.Trace(err)
			}
			colPosStr = " " + colPosStr
		}
		return fmt.Sprintf("ADD COLUMN %s", colDefStr+colPosStr), nil
	case ast.AlterTableAddConstraint:
		return genEmptySQL(strconv.Itoa(int(alterSpec.Tp))), nil
	case ast.AlterTableDropColumn:
		col := alterSpec.OldColumnName.Name.L
		return fmt.Sprintf("DROP COLUMN `%s`", col), nil
	case ast.AlterTableDropPrimaryKey:
		return genEmptySQL(strconv.Itoa(int(alterSpec.Tp))), nil
	case ast.AlterTableDropIndex:
		return genEmptySQL(strconv.Itoa(int(alterSpec.Tp))), nil
	case ast.AlterTableDropForeignKey:
		return genEmptySQL(strconv.Itoa(int(alterSpec.Tp))), nil
	case ast.AlterTableChangeColumn:
		oldColName := alterSpec.OldColumnName.Name.L
		newColName := alterSpec.NewColumns[0].Name.Name.L
		if oldColName != newColName {
			return "", errors.New("Don't support rename column: " + alterSpec.Text())
		}
		return analyzeModifyColumn(alterSpec)
	case ast.AlterTableModifyColumn:
		return analyzeModifyColumn(alterSpec)
	case ast.AlterTableAlterColumn:
		return genEmptySQL(strconv.Itoa(int(alterSpec.Tp))), nil
	case ast.AlterTableLock:
		return genEmptySQL(strconv.Itoa(int(alterSpec.Tp))), nil
	default:
		return "", errors.New("Invalid alter table spec type code: " + strconv.Itoa(int(alterSpec.Tp)))
	}
}

func analyzeModifyColumn(alterSpec *ast.AlterTableSpec) (string, error) {
	var colDefStr = ""
	var colPosStr = ""
	var err error = nil
	colDefStr, err = analyzeColumnDef(alterSpec.NewColumns[0], "")
	if err != nil {
		return "", errors.Trace(err)
	}
	if alterSpec.Position != nil && alterSpec.Position.Tp != ast.ColumnPositionNone {
		colPosStr, err = analyzeColumnPosition(alterSpec.Position)
		if err != nil {
			return "", errors.Trace(err)
		}
		colPosStr = " " + colPosStr
	}
	return fmt.Sprintf("MODIFY COLUMN %s", colDefStr+colPosStr), nil
}

// Refer to https://dev.mysql.com/doc/refman/5.7/en/integer-types.html
// https://clickhouse.yandex/docs/en/data_types/
func analyzeColumnDef(colDef *ast.ColumnDef, pkColumn string) (string, error) {
	cName := colDef.Name.Name.L

	tp := colDef.Tp
	var typeStr = ""
	var typeStrFormat = "%s"
	unsigned := mysql.HasUnsignedFlag(tp.Flag)
	nullable := cName != pkColumn && isNullable(colDef)
	if nullable {
		typeStrFormat = "Nullable(%s)"
	}
	switch tp.Tp {
	case mysql.TypeBit: // bit
		typeStr = fmt.Sprintf(typeStrFormat, "UInt64")
	case mysql.TypeTiny: // tinyint
		if unsigned {
			typeStr = fmt.Sprintf(typeStrFormat, "UInt8")
		} else {
			typeStr = fmt.Sprintf(typeStrFormat, "Int8")
		}
	case mysql.TypeShort: // smallint
		if unsigned {
			typeStr = fmt.Sprintf(typeStrFormat, "UInt16")
		} else {
			typeStr = fmt.Sprintf(typeStrFormat, "Int16")
		}
	case mysql.TypeYear:
		typeStr = fmt.Sprintf(typeStrFormat, "Int16")
	case mysql.TypeLong, mysql.TypeInt24: // int, mediumint
		if unsigned {
			typeStr = fmt.Sprintf(typeStrFormat, "UInt32")
		} else {
			typeStr = fmt.Sprintf(typeStrFormat, "Int32")
		}
	case mysql.TypeFloat:
		typeStr = fmt.Sprintf(typeStrFormat, "Float32")
	case mysql.TypeDouble, mysql.TypeNewDecimal, mysql.TypeDecimal:
		typeStr = fmt.Sprintf(typeStrFormat, "Float64")
	case mysql.TypeTimestamp, mysql.TypeDatetime: // timestamp, datetime
		typeStr = fmt.Sprintf(typeStrFormat, "DateTime")
	case mysql.TypeDuration: // duration
		typeStr = fmt.Sprintf(typeStrFormat, "Int64")
	case mysql.TypeLonglong:
		if unsigned {
			typeStr = fmt.Sprintf(typeStrFormat, "UInt64")
		} else {
			typeStr = fmt.Sprintf(typeStrFormat, "Int64")
		}
	case mysql.TypeDate, mysql.TypeNewDate:
		typeStr = fmt.Sprintf(typeStrFormat, "Date")
	case mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString:
		typeStr = fmt.Sprintf(typeStrFormat, "String")
	case mysql.TypeString:
		fs := fmt.Sprintf("FixedString(%d)", tp.Flen)
		typeStr = fmt.Sprintf(typeStrFormat, fs)
	case mysql.TypeEnum:
		enumStr := ""
		format := "Enum16(''=0,%s)"
		for i, elem := range tp.Elems {
			if len(elem) == 0 {
				// Don't append item empty enum if there is already one specified by user.
				format = "Enum16(%s)"
			}
			if i == 0 {
				enumStr = fmt.Sprintf("'%s'=%d", elem, i+1)
			} else {
				enumStr = fmt.Sprintf("%s,'%s'=%d", enumStr, elem, i+1)
			}
		}
		enumStr = fmt.Sprintf(format, enumStr)
		typeStr = fmt.Sprintf(typeStrFormat, enumStr)
	case mysql.TypeSet, mysql.TypeJSON:
		typeStr = fmt.Sprintf(typeStrFormat, "String")
	// case mysql.TypeGeometry:
	// TiDB doesn't have Geometry type so we don't really need to handle it.
	default:
		return "", errors.New("Don't support type : " + tp.String())
	}

	colDefStr := fmt.Sprintf("`%s` %s", cName, typeStr)

	for _, option := range colDef.Options {
		if option.Tp == ast.ColumnOptionDefaultValue {
			if defaultValue, shouldQuote, err := formatFlashLiteral(option.Expr, colDef.Tp); err != nil {
				log.Warnf("Cannot compile column %s default value: %s", cName, err)
			} else {
				if shouldQuote {
					// Do final quote for string types. As we want to quote values like -255, which is hard to quote in lower level.
					defaultValue = fmt.Sprintf("'%s'", defaultValue)
				}
				colDefStr = fmt.Sprintf("%s DEFAULT %s", colDefStr, defaultValue)
			}
			break
		}
	}

	return colDefStr, nil
}

func findColumnInfo(tableInfo *model.TableInfo, colDef *ast.ColumnDef) *model.ColumnInfo {
	for _, colInfo := range tableInfo.Columns {
		if colInfo.Name.L == colDef.Name.Name.L {
			return colInfo
		}
	}
	return nil
}

func analyzeColumnPosition(cp *ast.ColumnPosition) (string, error) {
	switch cp.Tp {
	// case ast.ColumnPositionFirst:
	case ast.ColumnPositionAfter:
		return fmt.Sprintf("AFTER `%s`", cp.RelativeColumn.Name.L), nil
	default:
		return "", errors.New("Invalid column position code: " + strconv.Itoa(int(cp.Tp)))
	}
}

// mysqlTranslator translates TiDB binlog to mysql sqls
type flashTranslator struct{}

func init() {
	Register("flash", &flashTranslator{})
}

// Config set the configuration
func (f *flashTranslator) SetConfig(bool, bool) {
}

func (f *flashTranslator) GenInsertSQLs(schema string, table *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, error) {
	schema = strings.ToLower(schema)
	if f.pkHandleColumn(table) == nil {
		fakeImplicitColumn(table)
	}
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	keys := make([][]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	delFlag := 0

	colsTypeMap := toFlashColumnTypeMap(columns)
	columnList := f.genColumnList(columns)
	// addition 2 holder is for del flag and version
	columnPlaceholders := dml.GenColumnPlaceholders(len(columns) + 2)
	sql := fmt.Sprintf("IMPORT INTO `%s`.`%s` (%s) values (%s);", schema, table.Name, columnList, columnPlaceholders)

	for _, row := range rows {
		//decode the pk value
		remain, pk, err := codec.DecodeOne(row)
		hashKey := pk.GetInt64()
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		columnValues, err := tablecodec.DecodeRow(remain, colsTypeMap, gotime.Local)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		if columnValues == nil {
			columnValues = make(map[int64]types.Datum)
		}

		var vals []interface{}
		vals = append(vals, hashKey)
		for _, col := range columns {
			if isPKHandleColumn(table, col) {
				columnValues[col.ID] = pk
				vals = append(vals, pk.GetValue())
				continue
			}

			val, ok := columnValues[col.ID]
			if !ok {
				vals = append(vals, col.DefaultValue)
			} else {
				value, err := formatFlashData(val, col.FieldType)
				if err != nil {
					return nil, nil, nil, errors.Trace(err)
				}

				vals = append(vals, value.GetValue())
			}
		}
		vals = append(vals, commitTS)
		vals = append(vals, delFlag)

		if columnValues == nil {
			log.Warn("columnValues is nil")
			continue
		}

		sqls = append(sqls, sql)
		values = append(values, vals)
		var key []string
		// generate dispatching key
		// find primary keys
		key, err = f.generateDispatchKey(table, columnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		keys = append(keys, key)
	}

	return sqls, keys, values, nil
}

func makeRow(pk int64, values []interface{}, delFlag int, commitTS int64) []interface{} {
	var row []interface{}
	row = append(row, pk)
	row = append(row, values...)
	row = append(row, commitTS)
	row = append(row, delFlag)
	return row
}

func (f *flashTranslator) GenUpdateSQLs(schema string, table *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, error) {
	schema = strings.ToLower(schema)
	pkColumn := f.pkHandleColumn(table)
	if pkColumn == nil {
		pkColumn = fakeImplicitColumn(table)
	}
	pkID := pkColumn.ID
	sqls := make([]string, 0, len(rows))
	keys := make([][]string, 0, len(rows))
	totalValues := make([][]interface{}, 0, len(rows))
	colsTypeMap := toColumnTypeMap(table.Columns)

	for _, row := range rows {
		var updateColumns []*model.ColumnInfo
		var newValues []interface{}

		// TODO: Make updating pk working
		_, newColumnValues, err := decodeFlashOldAndNewRow(row, colsTypeMap, gotime.Local)
		newPkValue := newColumnValues[pkID]

		if err != nil {
			return nil, nil, nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name)
		}

		if len(newColumnValues) == 0 {
			continue
		}

		updateColumns, newValues, err = f.generateColumnAndValue(table.Columns, newColumnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		// TODO: confirm column list should be the same across update
		columnList := f.genColumnList(updateColumns)
		// addition 2 holder is for del flag and version
		columnPlaceholders := dml.GenColumnPlaceholders(len(table.Columns) + 2)

		sql := fmt.Sprintf("IMPORT INTO `%s`.`%s` (%s) values (%s);", schema, table.Name, columnList, columnPlaceholders)

		sqls = append(sqls, sql)
		totalValues = append(totalValues, makeRow(newPkValue.GetInt64(), newValues, 0, commitTS))

		// generate dispatching key
		// find primary keys
		key, err := f.generateDispatchKey(table, newColumnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		keys = append(keys, key)

	}

	return sqls, keys, totalValues, nil
}

func (f *flashTranslator) GenDeleteSQLs(schema string, table *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, error) {
	schema = strings.ToLower(schema)
	pkColumn := f.pkHandleColumn(table)
	if pkColumn == nil {
		pkColumn = fakeImplicitColumn(table)
	}
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	keys := make([][]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	colsTypeMap := toColumnTypeMap(columns)

	for _, row := range rows {
		columnValues, err := tablecodec.DecodeRow(row, colsTypeMap, gotime.Local)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		if columnValues == nil {
			continue
		}

		sql, value, key, err := f.genDeleteSQL(schema, table, pkColumn.ID, columnValues, commitTS)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		values = append(values, value)
		sqls = append(sqls, sql)
		keys = append(keys, key)
	}

	return sqls, keys, values, nil
}

func (f *flashTranslator) genDeleteSQL(schema string, table *model.TableInfo, pkID int64, columnValues map[int64]types.Datum, commitTS int64) (string, []interface{}, []string, error) {
	columns := table.Columns
	pk := columnValues[pkID]
	hashKey := pk.GetInt64()
	delFlag := 1
	oldColumns, value, err := f.generateColumnAndValue(columns, columnValues)
	var pkValue []interface{}
	pkValue = append(pkValue, hashKey)
	value = append(pkValue, value...)
	if err != nil {
		return "", nil, nil, errors.Trace(err)
	}
	columnList := f.genColumnList(oldColumns)
	columnPlaceholders := dml.GenColumnPlaceholders(len(oldColumns) + 2)

	key, err := f.generateDispatchKey(table, columnValues)
	if err != nil {
		return "", nil, nil, errors.Trace(err)
	}

	sql := fmt.Sprintf("IMPORT INTO `%s`.`%s` (%s) values (%s);", schema, table.Name, columnList, columnPlaceholders)

	value = append(value, commitTS)
	value = append(value, delFlag)
	return sql, value, key, nil
}

func (f *flashTranslator) GenDDLSQL(sql string, schema string, commitTS int64) (string, error) {
	schema = strings.ToLower(schema)
	stmts, err := parser.New().Parse(sql, "", "")
	if err != nil {
		return "", errors.Trace(err)
	}

	stmt := stmts[0]
	switch stmt.(type) {
	case *ast.CreateDatabaseStmt:
		createDatabaseStmt, _ := stmt.(*ast.CreateDatabaseStmt)
		return extractCreateDatabase(createDatabaseStmt)
	case *ast.DropDatabaseStmt:
		dropDatabaseStmt, _ := stmt.(*ast.DropDatabaseStmt)
		return extractDropDatabase(dropDatabaseStmt)
	case *ast.DropTableStmt:
		dropTableStmt, _ := stmt.(*ast.DropTableStmt)
		return extractDropTable(dropTableStmt, schema)
	case *ast.CreateTableStmt:
		createTableStmt, _ := stmt.(*ast.CreateTableStmt)
		return extractCreateTable(createTableStmt, schema)
	case *ast.AlterTableStmt:
		alterTableStmt, _ := stmt.(*ast.AlterTableStmt)
		return extractAlterTable(alterTableStmt, schema)
	case *ast.RenameTableStmt:
		renameTableStmt, _ := stmt.(*ast.RenameTableStmt)
		return extractRenameTable(renameTableStmt, schema)
	default:
		// TODO: hacking around empty sql, should bypass in upper level
		return genEmptySQL(sql), nil
	}
}

func (f *flashTranslator) genColumnList(columns []*model.ColumnInfo) string {
	var columnList []byte
	for _, column := range columns {
		name := fmt.Sprintf("`%s`", column.Name.L)
		columnList = append(columnList, []byte(name)...)

		columnList = append(columnList, ',')
	}
	colVersion := fmt.Sprintf("`%s`,", "_INTERNAL_VERSION")
	columnList = append(columnList, []byte(colVersion)...)

	colDelFlag := fmt.Sprintf("`%s`", "_INTERNAL_DELMARK")
	columnList = append(columnList, []byte(colDelFlag)...)

	return string(columnList)
}

func (f *flashTranslator) genColumnPlaceholders(length int) string {
	values := make([]string, length, length)
	for i := 0; i < length; i++ {
		values[i] = "?"
	}
	return strings.Join(values, ",")
}

func (f *flashTranslator) pkHandleColumn(table *model.TableInfo) *model.ColumnInfo {
	for _, col := range table.Columns {
		if isPKHandleColumn(table, col) {
			return col
		}
	}

	return nil
}

func (f *flashTranslator) pkIndexColumns(table *model.TableInfo) ([]*model.ColumnInfo, error) {
	col := f.pkHandleColumn(table)
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

func (f *flashTranslator) generateColumnAndValue(columns []*model.ColumnInfo, columnValues map[int64]types.Datum) ([]*model.ColumnInfo, []interface{}, error) {
	var newColumn []*model.ColumnInfo
	var newColumnsValues []interface{}

	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if ok {
			newColumn = append(newColumn, col)
			value, err := formatFlashData(val, col.FieldType)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}

			newColumnsValues = append(newColumnsValues, value.GetValue())
		}
	}

	return newColumn, newColumnsValues, nil
}

func formatFlashData(data types.Datum, ft types.FieldType) (types.Datum, error) {
	if data.GetValue() == nil {
		return data, nil
	}

	switch ft.Tp {
	case mysql.TypeBit:
		ui, err := data.GetMysqlBit().ToInt()
		if err != nil {
			return data, errors.Trace(err)
		}
		return types.NewDatum(ui), nil
	case mysql.TypeDuration:
		// Duration is represented as gotime.Duration(int64), store it directly into CH.
		num, err := data.GetMysqlDuration().ToNumber().ToInt()
		if err != nil {
			log.Warnf("Corrupted Duration data: %v, will leave it zero.", data.GetMysqlDuration())
			num = 0
		}
		return types.NewDatum(num), nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		// TiDB won't accept invalid date/time EXCEPT "0000-00-00", which is default value for not-null columns. So deal with it specially.
		if data.GetMysqlTime().IsZero() {
			data = types.NewDatum(0)
		} else {
			// To be very safe, transform to go time first to normalize invalid date/time.
			mysqlTime := data.GetMysqlTime().Time
			// Using UTC timezone
			timezone := gotime.UTC
			// Need to consider timezone for timestamp.
			if ft.Tp == mysql.TypeTimestamp {
				timezone = gotime.Local
			}
			goTime := gotime.Date(mysqlTime.Year(), gotime.Month(mysqlTime.Month()), mysqlTime.Day(), mysqlTime.Hour(), mysqlTime.Minute(), mysqlTime.Second(), mysqlTime.Microsecond()*1000, timezone)
			data = types.NewDatum(goTime.Unix())
		}
	case mysql.TypeDecimal, mysql.TypeNewDecimal:
		data = types.NewDatum(fmt.Sprintf("%v", data.GetValue()))
	case mysql.TypeEnum:
		data = types.NewDatum(data.GetMysqlEnum().Name)
	case mysql.TypeSet:
		data = types.NewDatum(data.GetMysqlSet().String())
	case mysql.TypeJSON:
		data = types.NewDatum(data.GetMysqlJSON().String())
	case mysql.TypeGeometry:
		// TiDB doesn't have Geometry type, so put it null.
		data = types.NewDatum(nil)
	}

	return data, nil
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
		} else {
			return "", false, errors.New(fmt.Sprintf("Function expression %s is not supported.", e.FnName))
		}
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

	return "", false, errors.New("Shouldn't reach here.")
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

func (f *flashTranslator) generateDispatchKey(table *model.TableInfo, columnValues map[int64]types.Datum) ([]string, error) {
	var columnsValues []string
	columns, err := f.pkIndexColumns(table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if ok {
			value, err := formatFlashData(val, col.FieldType)
			if err != nil {
				return nil, errors.Trace(err)
			}
			columnsValues = append(columnsValues, fmt.Sprintf("%s", value.GetValue()))
		} else {
			columnsValues = append(columnsValues, fmt.Sprintf("%s", col.DefaultValue))
		}
	}

	if len(columnsValues) == 0 {
		columnsValues = append(columnsValues, table.Name.O)
	}
	return columnsValues, nil
}

func toFlashColumnTypeMap(columns []*model.ColumnInfo) map[int64]*types.FieldType {
	colTypeMap := make(map[int64]*types.FieldType)
	for _, col := range columns {
		colTypeMap[col.ID] = &col.FieldType
	}

	return colTypeMap
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
