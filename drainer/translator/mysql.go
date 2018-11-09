package translator

import (
	"bytes"
	"fmt"
	"time"

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

const implicitColID = -1

// mysqlTranslator translates TiDB binlog to mysql sqls
type mysqlTranslator struct {
	// hasImplicitCol is used for tidb implicit column
	hasImplicitCol bool

	// if set true, will generate SQL like 'insert into ...' for insert. otherwise, will generate SQL like 'replace into ...'
	useInsert bool
}

func init() {
	Register("mysql", &mysqlTranslator{})
	Register("tidb", &mysqlTranslator{})
}

func (m *mysqlTranslator) SetConfig(hasImplicitCol, useInsert bool) {
	m.hasImplicitCol = hasImplicitCol
	m.useInsert = useInsert
}

func (m *mysqlTranslator) GenInsertSQLs(schema string, table *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	keys := make([][]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))

	colsTypeMap := util.ToColumnTypeMap(columns)
	columnList := m.genColumnList(columns)
	columnPlaceholders := dml.GenColumnPlaceholders((len(columns)))

	insertStr := "replace"
	if m.useInsert {
		insertStr = "insert"
	}
	sql := fmt.Sprintf("%s into `%s`.`%s` (%s) values (%s);", insertStr, schema, table.Name, columnList, columnPlaceholders)

	for _, row := range rows {
		//decode the pk value
		remain, pk, err := codec.DecodeOne(row)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		columnValues, err := tablecodec.DecodeRow(remain, colsTypeMap, time.Local)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		if columnValues == nil {
			columnValues = make(map[int64]types.Datum)
		}

		var vals []interface{}
		for _, col := range columns {
			if IsPKHandleColumn(table, col) {
				columnValues[col.ID] = pk
				vals = append(vals, pk.GetValue())
				continue
			}

			val, ok := columnValues[col.ID]
			if !ok {
				vals = append(vals, col.DefaultValue)
			} else {
				value, err := formatData(val, col.FieldType)
				if err != nil {
					return nil, nil, nil, errors.Trace(err)
				}

				vals = append(vals, value.GetValue())
			}
		}

		if columnValues == nil {
			log.Warn("columnValues is nil")
			continue
		}

		sqls = append(sqls, sql)
		values = append(values, vals)
		// generate dispatching key
		// find primary keys
		key, err := m.generateDispatchKey(table, columnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		keys = append(keys, key)
	}

	return sqls, keys, values, nil
}

func (m *mysqlTranslator) GenUpdateSQLs(schema string, table *model.TableInfo, rows [][]byte, commitTS int64, safeMode bool) ([]string, [][]string, [][]interface{}, error) {
	if safeMode {
		return m.genUpdateSQLsSafeMode(schema, table, rows, commitTS)
	}

	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	keys := make([][]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	colsTypeMap := util.ToColumnTypeMap(columns)

	for _, row := range rows {
		var updateColumns []*model.ColumnInfo
		var oldValues []interface{}
		var newValues []interface{}

		oldColumnValues, newColumnValues, err := DecodeOldAndNewRow(row, colsTypeMap, time.Local)
		if err != nil {
			return nil, nil, nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name)
		}

		if len(newColumnValues) == 0 {
			continue
		}

		updateColumns, oldValues, err = m.generateColumnAndValue(columns, oldColumnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		whereColumns := updateColumns

		updateColumns, newValues, err = m.generateColumnAndValue(columns, newColumnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		var value []interface{}
		kvs := m.genKVs(updateColumns)
		value = append(value, newValues...)

		var where string
		where, oldValues, err = m.genWhere(table, whereColumns, oldValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		value = append(value, oldValues...)
		sql := fmt.Sprintf("update `%s`.`%s` set %s where %s limit 1;", schema, table.Name, kvs, where)
		sqls = append(sqls, sql)
		values = append(values, value)

		// generate dispatching key
		// find primary keys
		oldKey, err := m.generateDispatchKey(table, oldColumnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		newKey, err := m.generateDispatchKey(table, newColumnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		key := append(newKey, oldKey...)
		keys = append(keys, key)
	}

	return sqls, keys, values, nil
}

func (m *mysqlTranslator) genUpdateSQLsSafeMode(schema string, table *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	keys := make([][]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	colsTypeMap := util.ToColumnTypeMap(columns)
	columnList := m.genColumnList(columns)
	columnPlaceholders := dml.GenColumnPlaceholders(len(columns))

	for _, row := range rows {
		oldColumnValues, newColumnValues, err := DecodeOldAndNewRow(row, colsTypeMap, time.Local)
		if err != nil {
			return nil, nil, nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name)
		}

		if len(newColumnValues) == 0 {
			continue
		}

		var newValues []interface{}
		_, newValues, err = m.generateColumnAndValue(columns, newColumnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		// generate delete sql
		deleteSQL, deleteValue, deleteKey, err := m.genDeleteSQL(schema, table, oldColumnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		sqls = append(sqls, deleteSQL)
		values = append(values, deleteValue)

		// generate insert sql
		insertStr := "replace"
		if m.useInsert {
			insertStr = "insert"
		}
		replaceSQL := fmt.Sprintf("%s into `%s`.`%s` (%s) values (%s);", insertStr, schema, table.Name, columnList, columnPlaceholders)
		sqls = append(sqls, replaceSQL)
		values = append(values, newValues)

		// generate dispatching key
		// find primary keys
		replaceKey, err := m.generateDispatchKey(table, newColumnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		key := append(deleteKey, replaceKey...)
		// one is for delete sql, another for replace sql
		keys = append(keys, key)
		keys = append(keys, key)
	}

	log.Infof("safe mode sqls: %+v", sqls)
	return sqls, keys, values, nil
}

func (m *mysqlTranslator) GenDeleteSQLs(schema string, table *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	keys := make([][]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	colsTypeMap := util.ToColumnTypeMap(columns)

	for _, row := range rows {
		columnValues, err := tablecodec.DecodeRow(row, colsTypeMap, time.Local)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		if columnValues == nil {
			continue
		}

		sql, value, key, err := m.genDeleteSQL(schema, table, columnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		values = append(values, value)
		sqls = append(sqls, sql)
		keys = append(keys, key)
	}

	return sqls, keys, values, nil
}

func (m *mysqlTranslator) genDeleteSQL(schema string, table *model.TableInfo, columnValues map[int64]types.Datum) (string, []interface{}, []string, error) {
	columns := table.Columns

	whereColumns, value, err := m.generateColumnAndValue(columns, columnValues)
	if err != nil {
		return "", nil, nil, errors.Trace(err)
	}

	where, value, err := m.genWhere(table, whereColumns, value)
	if err != nil {
		return "", nil, nil, errors.Trace(err)
	}

	// generate dispatching key
	// find primary keys
	key, err := m.generateDispatchKey(table, columnValues)
	if err != nil {
		return "", nil, nil, errors.Trace(err)
	}

	sql := fmt.Sprintf("delete from `%s`.`%s` where %s limit 1;", schema, table.Name, where)

	return sql, value, key, nil
}

func (m *mysqlTranslator) GenDDLSQL(sql string, schema string, commitTS int64) (string, error) {
	stmts, err := parser.New().Parse(sql, "", "")
	if err != nil {
		return "", errors.Trace(err)
	}

	stmt := stmts[0]
	_, isCreateDatabase := stmt.(*ast.CreateDatabaseStmt)
	if isCreateDatabase {
		return fmt.Sprintf("%s;", sql), nil
	}

	return fmt.Sprintf("use `%s`; %s;", schema, sql), nil
}

func (m *mysqlTranslator) genWhere(table *model.TableInfo, columns []*model.ColumnInfo, data []interface{}) (string, []interface{}, error) {
	var kvs bytes.Buffer
	// if has unique key, use it to construct where condition
	ucs, err := m.uniqueIndexColumns(table, false)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	hasUK := (len(ucs) != 0)
	ucsMap := make(map[int64]*model.ColumnInfo)
	for _, col := range ucs {
		ucsMap[col.ID] = col
	}

	var conditionValues []interface{}
	first := true
	for i, col := range columns {
		_, ok := ucsMap[col.ID]
		if !ok && hasUK {
			// if table has primary/unique key, just ignore the non primary/unique key column
			continue
		}

		valueClause := "= ?"
		if data[i] == nil {
			valueClause = "is NULL"
		} else {
			conditionValues = append(conditionValues, data[i])
		}

		if first {
			first = false
			fmt.Fprintf(&kvs, "`%s` %s", columns[i].Name, valueClause)
		} else {
			fmt.Fprintf(&kvs, " and `%s` %s", columns[i].Name, valueClause)
		}
	}

	return kvs.String(), conditionValues, nil
}

func (m *mysqlTranslator) genColumnList(columns []*model.ColumnInfo) string {
	var columnList []byte
	for i, column := range columns {
		name := fmt.Sprintf("`%s`", column.Name)
		columnList = append(columnList, []byte(name)...)

		if i != len(columns)-1 {
			columnList = append(columnList, ',')
		}
	}

	return string(columnList)
}

func (m *mysqlTranslator) genKVs(columns []*model.ColumnInfo) string {
	var kvs bytes.Buffer
	for i := range columns {
		if i == len(columns)-1 {
			fmt.Fprintf(&kvs, "`%s` = ?", columns[i].Name)
		} else {
			fmt.Fprintf(&kvs, "`%s` = ?, ", columns[i].Name)
		}
	}

	return kvs.String()
}

func (m *mysqlTranslator) pkHandleColumn(table *model.TableInfo) *model.ColumnInfo {
	for _, col := range table.Columns {
		if IsPKHandleColumn(table, col) {
			return col
		}
	}

	return nil
}

func (m *mysqlTranslator) uniqueIndexColumns(table *model.TableInfo, findAll bool) ([]*model.ColumnInfo, error) {
	// pkHandleCol may in table.Indices, use map to keep olny one same key.
	uniqueColsMap := make(map[string]interface{})
	uniqueCols := make([]*model.ColumnInfo, 0, 2)

	pkHandleCol := m.pkHandleColumn(table)
	if pkHandleCol != nil {
		uniqueColsMap[pkHandleCol.Name.O] = pkHandleCol
		uniqueCols = append(uniqueCols, pkHandleCol)

		if !findAll {
			return uniqueCols, nil
		}
	}

	columns := make(map[string]*model.ColumnInfo)
	for _, col := range table.Columns {
		columns[col.Name.O] = col
	}

	for _, idx := range table.Indices {
		if idx.Primary || idx.Unique {
			for _, col := range idx.Columns {
				if column, ok := columns[col.Name.O]; ok {
					if _, ok := uniqueColsMap[col.Name.O]; !ok {
						uniqueColsMap[col.Name.O] = column
						uniqueCols = append(uniqueCols, column)
					}
				}
			}

			if len(uniqueCols) == 0 {
				return nil, errors.New("primay/unique index is empty, but should not be empty")
			}

			if !findAll {
				return uniqueCols, nil
			}
		}
	}

	return uniqueCols, nil
}

// IsPKHandleColumn check if the column if the pk handle of tidb
func IsPKHandleColumn(table *model.TableInfo, column *model.ColumnInfo) bool {
	return (mysql.HasPriKeyFlag(column.Flag) && table.PKIsHandle) || column.ID == implicitColID
}

func (m *mysqlTranslator) generateColumnAndValue(columns []*model.ColumnInfo, columnValues map[int64]types.Datum) ([]*model.ColumnInfo, []interface{}, error) {
	var newColumn []*model.ColumnInfo
	var newColumnsValues []interface{}

	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if ok {
			newColumn = append(newColumn, col)
			value, err := formatData(val, col.FieldType)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}

			newColumnsValues = append(newColumnsValues, value.GetValue())
		}
	}

	return newColumn, newColumnsValues, nil
}

func (m *mysqlTranslator) generateDispatchKey(table *model.TableInfo, columnValues map[int64]types.Datum) ([]string, error) {
	var columnsValues []string
	columns, err := m.uniqueIndexColumns(table, true)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if ok {
			value, err := formatData(val, col.FieldType)
			if err != nil {
				return nil, errors.Trace(err)
			}

			columnsValues = append(columnsValues, fmt.Sprintf("[%s: %v]", col.Name, value.GetValue()))
		} else {
			columnsValues = append(columnsValues, fmt.Sprintf("[%s: %v]", col.Name, col.DefaultValue))
		}
	}
	return columnsValues, nil
}

func formatData(data types.Datum, ft types.FieldType) (types.Datum, error) {
	if data.GetValue() == nil {
		return data, nil
	}

	switch ft.Tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeDecimal, mysql.TypeNewDecimal, mysql.TypeJSON:
		data = types.NewDatum(fmt.Sprintf("%v", data.GetValue()))
	case mysql.TypeEnum:
		data = types.NewDatum(data.GetMysqlEnum().Value)
	case mysql.TypeSet:
		data = types.NewDatum(data.GetMysqlSet().Value)
	case mysql.TypeBit:
		data = types.NewDatum(data.GetBytes())
	}

	return data, nil
}

// DecodeOldAndNewRow decodes a byte slice into datums with a existing row map.
// Row layout: colID1, value1, colID2, value2, .....
func DecodeOldAndNewRow(b []byte, cols map[int64]*types.FieldType, loc *time.Location) (map[int64]types.Datum, map[int64]types.Datum, error) {
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
		return nil, nil, errors.Errorf(" row data is corruption %v", b)
	}

	return oldRow, newRow, nil
}
