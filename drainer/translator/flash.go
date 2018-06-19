package translator

import (
	"fmt"
	"strconv"
	"strings"
	gotime "time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/dml"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

// flashTranslator translates TiDB binlog to flash sqls
type flashTranslator struct{}

func init() {
	Register("flash", &flashTranslator{})
}

// Config set the configuration
func (f *flashTranslator) SetConfig(bool, bool) {
}

func (f *flashTranslator) GenInsertSQLs(schema string, table *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, error) {
	schema = strings.ToLower(schema)
	if pkHandleColumn(table) == nil {
		fakeImplicitColumn(table)
	}
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	keys := make([][]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	version := makeInternalVersionValue(uint64(commitTS))
	delFlag := makeInternalDelmarkValue(false)

	colsTypeMap := toFlashColumnTypeMap(columns)
	columnList := genColumnList(columns)
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
			if IsPKHandleColumn(table, col) {
				columnValues[col.ID] = pk
				pkVal, err := formatFlashData(pk, col.FieldType)
				if err != nil {
					return nil, nil, nil, errors.Trace(err)
				}
				vals = append(vals, pkVal)
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

				vals = append(vals, value)
			}
		}
		vals = append(vals, version)
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
		key, err = genDispatchKey(table, columnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		keys = append(keys, key)
	}

	return sqls, keys, values, nil
}

func (f *flashTranslator) GenUpdateSQLs(schema string, table *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, error) {
	schema = strings.ToLower(schema)
	pkColumn := pkHandleColumn(table)
	if pkColumn == nil {
		pkColumn = fakeImplicitColumn(table)
	}
	pkID := pkColumn.ID
	sqls := make([]string, 0, len(rows))
	keys := make([][]string, 0, len(rows))
	totalValues := make([][]interface{}, 0, len(rows))
	colsTypeMap := util.ToColumnTypeMap(table.Columns)
	version := makeInternalVersionValue(uint64(commitTS))
	delFlag := makeInternalDelmarkValue(false)

	for _, row := range rows {
		var updateColumns []*model.ColumnInfo
		var newValues []interface{}

		// TODO: Make updating pk working
		oldColumnValues, newColumnValues, err := decodeFlashOldAndNewRow(row, colsTypeMap, gotime.Local)
		newPkValue := newColumnValues[pkID]

		if err != nil {
			return nil, nil, nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name)
		}

		if len(newColumnValues) == 0 {
			continue
		}

		updateColumns, newValues, err = genColumnAndValue(table.Columns, newColumnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		// TODO: confirm column list should be the same across update
		columnList := genColumnList(updateColumns)
		// addition 2 holder is for del flag and version
		columnPlaceholders := dml.GenColumnPlaceholders(len(table.Columns) + 2)

		sql := fmt.Sprintf("IMPORT INTO `%s`.`%s` (%s) values (%s);", schema, table.Name, columnList, columnPlaceholders)

		sqls = append(sqls, sql)
		totalValues = append(totalValues, makeRow(newPkValue.GetInt64(), newValues, version, delFlag))

		// generate dispatching key
		// find primary keys
		// generate dispatching key
		// find primary keys
		oldKey, err := genDispatchKey(table, oldColumnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		newKey, err := genDispatchKey(table, newColumnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		key := append(newKey, oldKey...)
		keys = append(keys, key)
	}

	return sqls, keys, totalValues, nil
}

func (f *flashTranslator) GenDeleteSQLs(schema string, table *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, error) {
	schema = strings.ToLower(schema)
	pkColumn := pkHandleColumn(table)
	if pkColumn == nil {
		pkColumn = fakeImplicitColumn(table)
	}
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	keys := make([][]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	colsTypeMap := util.ToColumnTypeMap(columns)

	for _, row := range rows {
		columnValues, err := tablecodec.DecodeRow(row, colsTypeMap, gotime.Local)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		if columnValues == nil {
			continue
		}

		sql, value, key, err := genDeleteSQL(schema, table, pkColumn.ID, columnValues, commitTS)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		values = append(values, value)
		sqls = append(sqls, sql)
		keys = append(keys, key)
	}

	return sqls, keys, values, nil
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
	case *ast.TruncateTableStmt:
		truncateTableStmt, _ := stmt.(*ast.TruncateTableStmt)
		return extractTruncateTable(truncateTableStmt, schema), nil
	default:
		// TODO: hacking around empty sql, should bypass in upper level
		return genEmptySQL(sql), nil
	}
}

func genDeleteSQL(schema string, table *model.TableInfo, pkID int64, columnValues map[int64]types.Datum, commitTS int64) (string, []interface{}, []string, error) {
	columns := table.Columns
	pk := columnValues[pkID]
	hashKey := pk.GetInt64()
	version := makeInternalVersionValue(uint64(commitTS))
	delFlag := makeInternalDelmarkValue(true)
	oldColumns, value, err := genColumnAndValue(columns, columnValues)
	var pkValue []interface{}
	pkValue = append(pkValue, hashKey)
	value = append(pkValue, value...)
	if err != nil {
		return "", nil, nil, errors.Trace(err)
	}
	columnList := genColumnList(oldColumns)
	columnPlaceholders := dml.GenColumnPlaceholders(len(oldColumns) + 2)

	key, err := genDispatchKey(table, columnValues)
	if err != nil {
		return "", nil, nil, errors.Trace(err)
	}

	sql := fmt.Sprintf("IMPORT INTO `%s`.`%s` (%s) values (%s);", schema, table.Name, columnList, columnPlaceholders)

	value = append(value, version)
	value = append(value, delFlag)
	return sql, value, key, nil
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

func extractTruncateTable(stmt *ast.TruncateTableStmt, schema string) string {
	tableName := stmt.Table.Name.L
	return fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`", schema, tableName)
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

// extract single row handle column, if implicit, generate one
func extractRowHandle(stmt *ast.CreateTableStmt) (colName string, explicitHandle bool) {
	constrains := stmt.Constraints
	columns := stmt.Cols
	var primaryCnt = 0
	var primaryColumn = ""
	for _, colDef := range columns {
		cNameLowercase := colDef.Name.Name.L
		if isPrimaryKeyColumn(colDef) {
			primaryCnt++
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

func analyzeAlterSpec(alterSpec *ast.AlterTableSpec) (string, error) {
	switch alterSpec.Tp {
	case ast.AlterTableOption:
		return genEmptySQL(strconv.Itoa(int(alterSpec.Tp))), nil
	case ast.AlterTableAddColumns:
		var colDefStr = ""
		var colPosStr = ""
		var err error
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
			return "", errors.NotSupportedf("Rename column: " + alterSpec.Text())
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
	var err error
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
		// Hacking around Date type, will recover after Date mapped to true CH Date.
		typeStr = fmt.Sprintf(typeStrFormat, hackDateTypeMapping())
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString:
		typeStr = fmt.Sprintf(typeStrFormat, "String")
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

func analyzeColumnPosition(cp *ast.ColumnPosition) (string, error) {
	switch cp.Tp {
	// case ast.ColumnPositionFirst:
	case ast.ColumnPositionAfter:
		return fmt.Sprintf("AFTER `%s`", cp.RelativeColumn.Name.L), nil
	default:
		return "", errors.New("Invalid column position code: " + strconv.Itoa(int(cp.Tp)))
	}
}

func genColumnList(columns []*model.ColumnInfo) string {
	var columnList []byte
	for _, column := range columns {
		name := fmt.Sprintf("`%s`", column.Name.L)
		columnList = append(columnList, []byte(name)...)

		columnList = append(columnList, ',')
	}
	colVersion := fmt.Sprintf("`%s`,", internalVersionColName)
	columnList = append(columnList, []byte(colVersion)...)

	colDelFlag := fmt.Sprintf("`%s`", internalDelmarkColName)
	columnList = append(columnList, []byte(colDelFlag)...)

	return string(columnList)
}

func genColumnAndValue(columns []*model.ColumnInfo, columnValues map[int64]types.Datum) ([]*model.ColumnInfo, []interface{}, error) {
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

			newColumnsValues = append(newColumnsValues, value)
		}
	}

	return newColumn, newColumnsValues, nil
}

func genDispatchKey(table *model.TableInfo, columnValues map[int64]types.Datum) ([]string, error) {
	return make([]string, 0), nil
}
