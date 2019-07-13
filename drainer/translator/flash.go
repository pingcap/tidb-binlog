// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package translator

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	gotime "time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-binlog/pkg/dml"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	tipb "github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
)

// GenFlashSQLs generate the SQL need to execute syncing this binlog to Flash
func GenFlashSQLs(infoGetter TableInfoGetter, pv *tipb.PrewriteValue, commitTS int64) (sqls []string, argss [][]interface{}, err error) {
	for _, mut := range pv.GetMutations() {
		var info *model.TableInfo
		var ok bool
		info, ok = infoGetter.TableByID(mut.GetTableId())
		if !ok {
			return nil, nil, errors.Errorf("TableByID empty table id: %d", mut.GetTableId())
		}

		var schema string
		schema, _, ok = infoGetter.SchemaAndTableName(mut.GetTableId())
		if !ok {
			return nil, nil, errors.Errorf("SchemaAndTableName empty table id: %d", mut.GetTableId())
		}

		iter := newSequenceIterator(&mut)
		for {
			mutType, row, err := iter.next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, nil, errors.Trace(err)
			}

			var sql string
			var args []interface{}
			switch mutType {
			case tipb.MutationType_Insert:
				sql, args, err = GenFlashInsertSQL(schema, info, row, commitTS)
				if err != nil {
					return nil, nil, errors.Annotate(err, "gen insert sql fail")
				}
			case tipb.MutationType_Update:
				sql, args, err = GenFlashUpdateSQL(schema, info, row, commitTS)
				if err != nil {
					return nil, nil, errors.Annotate(err, "gen update sql fail")
				}
			case tipb.MutationType_DeleteRow:
				sql, args, err = GenFlashDeleteSQL(schema, info, row, commitTS)
				if err != nil {
					return nil, nil, errors.Annotate(err, "gen delete sql fail")
				}
			default:
				return nil, nil, errors.Errorf("unknown mutation type: %v", mutType)
			}
			sqls = append(sqls, sql)
			argss = append(argss, args)
		}

	}

	return
}

// GenFlashInsertSQL generate the SQL need to execute syncing this insert row to Flash
func GenFlashInsertSQL(schema string, table *model.TableInfo, row []byte, commitTS int64) (sql string, args []interface{}, err error) {
	schema = strings.ToLower(schema)
	if pkHandleColumn(table) == nil {
		fakeImplicitColumn(table)
	}
	columns := writableColumns(table)
	version := makeInternalVersionValue(uint64(commitTS))
	delFlag := makeInternalDelmarkValue(false)

	columnList := genColumnList(columns)
	// addition 2 holder is for del flag and version
	columnPlaceholders := dml.GenColumnPlaceholders(len(columns) + 2)
	sql = fmt.Sprintf("IMPORT INTO `%s`.`%s` (%s) values (%s);", schema, table.Name.L, columnList, columnPlaceholders)

	//decode the pk value
	pk, columnValues, err := insertRowToDatums(table, row)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	hashKey := pk.GetInt64()

	var vals []interface{}
	vals = append(vals, hashKey)
	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if !ok {
			vals = append(vals, col.GetDefaultValue())
		} else {
			value, err := formatFlashData(&val, &col.FieldType)
			if err != nil {
				return "", nil, errors.Trace(err)
			}

			vals = append(vals, value)
		}
	}
	vals = append(vals, version)
	vals = append(vals, delFlag)

	if len(columnValues) == 0 {
		panic(errors.New("columnValues is nil"))
	}

	args = vals

	return
}

// GenFlashUpdateSQL generate the SQL need to execute syncing this update row to Flash
func GenFlashUpdateSQL(schema string, table *model.TableInfo, row []byte, commitTS int64) (sql string, args []interface{}, err error) {
	schema = strings.ToLower(schema)
	pkColumn := pkHandleColumn(table)
	if pkColumn == nil {
		pkColumn = fakeImplicitColumn(table)
	}
	pkID := pkColumn.ID

	updtDecoder := newUpdateDecoder(table)
	version := makeInternalVersionValue(uint64(commitTS))
	delFlag := makeInternalDelmarkValue(false)

	var updateColumns []*model.ColumnInfo
	var newValues []interface{}

	// TODO: Make updating pk working
	_, newColumnValues, err := updtDecoder.decode(row, gotime.Local)
	newPkValue := newColumnValues[pkID]

	if err != nil {
		return "", nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name.L)
	}

	updateColumns, newValues, err = genColumnAndValue(table.Columns, newColumnValues)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	// TODO: confirm column list should be the same across update
	columnList := genColumnList(updateColumns)
	// addition 2 holder is for del flag and version
	columnPlaceholders := dml.GenColumnPlaceholders(len(table.Columns) + 2)

	sql = fmt.Sprintf("IMPORT INTO `%s`.`%s` (%s) values (%s);", schema, table.Name.L, columnList, columnPlaceholders)

	args = makeRow(newPkValue.GetInt64(), newValues, version, delFlag)
	return
}

// GenFlashDeleteSQL generate the SQL need to execute syncing this delete row to Flash
func GenFlashDeleteSQL(schema string, table *model.TableInfo, row []byte, commitTS int64) (sql string, args []interface{}, err error) {
	schema = strings.ToLower(schema)
	pkColumn := pkHandleColumn(table)
	if pkColumn == nil {
		pkColumn = fakeImplicitColumn(table)
	}
	columns := table.Columns
	colsTypeMap := util.ToColumnTypeMap(columns)

	columnValues, err := tablecodec.DecodeRow(row, colsTypeMap, gotime.Local)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	sql, args, _, err = genDeleteSQL(schema, table, pkColumn.ID, columnValues, commitTS)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	return
}

// GenFlashDDLSQL generate the SQL need to execute syncing this DDL to Flash
func GenFlashDDLSQL(sql string, schema string) (string, error) {
	schema = strings.ToLower(schema)
	ddlParser := getParser()
	stmt, err := ddlParser.ParseOneStmt(sql, "", "")
	if err != nil {
		return "", errors.Annotatef(err, "parse sql failed: %s", sql)
	}

	switch stmt := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		return extractCreateDatabase(stmt)
	case *ast.DropDatabaseStmt:
		return extractDropDatabase(stmt)
	case *ast.DropTableStmt:
		return extractDropTable(stmt, schema)
	case *ast.CreateTableStmt:
		return extractCreateTable(stmt, schema)
	case *ast.AlterTableStmt:
		alterSQL, err := extractAlterTable(stmt, schema)
		if err != nil {
			return alterSQL, err
		}
		if len(alterSQL) == 0 {
			return genEmptySQL(sql), nil
		}
		return alterSQL, nil
	case *ast.RenameTableStmt:
		return extractRenameTable(stmt, schema)
	case *ast.TruncateTableStmt:
		return extractTruncateTable(stmt, schema), nil
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

	sql := fmt.Sprintf("IMPORT INTO `%s`.`%s` (%s) values (%s);", schema, table.Name.L, columnList, columnPlaceholders)

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
	tableName := stmt.Table.Name.L
	// create table like
	if stmt.ReferTable != nil {
		referTableSchema, referTableName := stmt.ReferTable.Schema.L, stmt.ReferTable.Name.L
		if len(referTableSchema) == 0 {
			referTableSchema = schema
		}
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` AS `%s`.`%s`", schema, tableName, referTableSchema, referTableName), nil
	}
	// extract primary key
	pkColumn, explicitHandle := extractRowHandle(stmt)
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
	specStrs := make([]string, 0, len(stmt.Specs))
	for _, spec := range stmt.Specs {
		specStr, err := analyzeAlterSpec(spec)
		if err != nil {
			return "", errors.Trace(err)
		}
		if len(specStr) != 0 {
			specStrs = append(specStrs, specStr)
		}
	}

	tableName := stmt.Table.Name.L
	if len(specStrs) == 0 {
		return "", nil
	}
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
		return "", nil
	case ast.AlterTableAddColumns:
		var colDefStr string
		var colPosStr string
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
		return "", nil
	case ast.AlterTableDropColumn:
		col := alterSpec.OldColumnName.Name.L
		return fmt.Sprintf("DROP COLUMN `%s`", col), nil
	case ast.AlterTableDropPrimaryKey:
		return "", nil
	case ast.AlterTableDropIndex:
		return "", nil
	case ast.AlterTableDropForeignKey:
		return "", nil
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
		return "", nil
	case ast.AlterTableLock:
		return "", nil
	default:
		return "", errors.New("Invalid alter table spec type code: " + strconv.Itoa(int(alterSpec.Tp)))
	}
}

func analyzeModifyColumn(alterSpec *ast.AlterTableSpec) (string, error) {
	var colDefStr string
	var colPosStr string
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
	case mysql.TypeDouble:
		typeStr = fmt.Sprintf(typeStrFormat, "Float64")
	case mysql.TypeNewDecimal, mysql.TypeDecimal:
		if tp.Flen == types.UnspecifiedLength {
			tp.Flen, _ = mysql.GetDefaultFieldLengthAndDecimal(tp.Tp)
		}
		if tp.Decimal == types.UnspecifiedLength {
			_, tp.Decimal = mysql.GetDefaultFieldLengthAndDecimal(tp.Tp)
		}
		decimalTypeStr := fmt.Sprintf("Decimal(%d, %d)", tp.Flen, tp.Decimal)
		typeStr = fmt.Sprintf(typeStrFormat, decimalTypeStr)
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
				log.Warn("Cannot compile column", zap.String("name", cName), zap.Error(err))
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
		colName := column.Name.L
		name := fmt.Sprintf("`%s`", colName)
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
			value, err := formatFlashData(&val, &col.FieldType)
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
