package translator

import (
	"bytes"
	"fmt"
	"strings"

	"time"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

// mysqlTranslator translates TiDB binlog to mysql sqls
type mysqlTranslator struct{}

func init() {
	Register("mysql", &mysqlTranslator{})
}

func (m *mysqlTranslator) GenInsertSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, []string, [][]interface{}, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	keys := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))

	columnList := m.genColumnList(columns)
	columnPlaceholders := m.genColumnPlaceholders((len(columns)))
	sql := fmt.Sprintf("replace into `%s`.`%s` (%s) values (%s);", schema, table.Name, columnList, columnPlaceholders)

	for _, row := range rows {
		//decode the pk value
		remain, pk, err := codec.DecodeOne(row)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		var r []types.Datum
		// decode the remain values, the format is [coldID, colVal, coldID, colVal....]
		// while the table just has primary id, filter the nil value that follows by the primary id
		if remain[0] != codec.NilFlag {
			r, err = codec.Decode(remain, 2*(len(columns)-1))
			if err != nil {
				return nil, nil, nil, errors.Trace(err)
			}
		}

		if len(r)%2 != 0 {
			return nil, nil, nil, errors.Errorf("table `%s`.`%s` insert row raw data is corruption %v", schema, table.Name, r)
		}

		var columnValues = make(map[int64]types.Datum)
		for i := 0; i < len(r); i += 2 {
			columnValues[r[i].GetInt64()] = r[i+1]
		}

		var vals []interface{}
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

				value, err := formatData(val, col.FieldType)
				if err != nil {
					return nil, nil, nil, errors.Trace(err)
				}

				vals = append(vals, value.GetValue())
			}
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

func (m *mysqlTranslator) GenUpdateSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, []string, [][]interface{}, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	keys := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))

	for _, row := range rows {
		var updateColumns []*model.ColumnInfo
		var oldValues []interface{}
		var newValues []interface{}

		r, err := codec.Decode(row, 2*len(columns))
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		if len(r)%2 != 0 {
			return nil, nil, nil, errors.Errorf("table `%s`.`%s` update row data is corruption %v", schema, table.Name, r)
		}

		var i int
		oldColumnValues := make(map[int64]types.Datum)
		for ; i < len(r)/2; i += 2 {
			oldColumnValues[r[i].GetInt64()] = r[i+1]
		}

		updateColumns, oldValues, err = m.generateColumnAndValue(columns, oldColumnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		whereColumns := updateColumns

		newColumnValues := make(map[int64]types.Datum)
		for ; i < len(r); i += 2 {
			newColumnValues[r[i].GetInt64()] = r[i+1]
		}

		if len(newColumnValues) == 0 {
			continue
		}

		updateColumns, newValues, err = m.generateColumnAndValue(columns, newColumnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		_, oldValues, err = m.genWhere(table, whereColumns, oldValues)
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
		keys = append(keys, deleteKey)

		// generate replace sql
		columnList := m.genColumnList(columns)
		columnPlaceholders := m.genColumnPlaceholders(len(columns))
		sql := fmt.Sprintf("replace into `%s`.`%s` (%s) values (%s);", schema, table.Name, columnList, columnPlaceholders)
		sqls = append(sqls, sql)
		values = append(values, newValues)
		key, err := m.generateDispatchKey(table, newColumnValues)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		keys = append(keys, key)

	}

	return sqls, keys, values, nil
}

func (m *mysqlTranslator) GenDeleteSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, []string, [][]interface{}, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	keys := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))

	for _, row := range rows {
		// var whereColumns []*model.ColumnInfo
		var value []interface{}
		r, err := codec.Decode(row, 2*len(columns))
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		if len(r)%2 != 0 {
			return nil, nil, nil, errors.Errorf("table `%s`.`%s` the delete row by cols binlog %v is courruption", schema, table.Name, r)
		}

		var columnValues = make(map[int64]types.Datum)
		for i := 0; i < len(r); i += 2 {
			columnValues[r[i].GetInt64()] = r[i+1]
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

func (m *mysqlTranslator) genDeleteSQL(schema string, table *model.TableInfo, columnValues map[int64]types.Datum ) (string, []interface{}, string, error) {
	columns := table.Columns
	whereColumns, value, err := m.generateColumnAndValue(columns, columnValues)
	if err != nil {
		return "", nil, "", errors.Trace(err)
	}

	var where string
	where, value, err = m.genWhere(table, whereColumns, value)
	if err != nil {
		return "", nil, "", errors.Trace(err)
	}

	key, err := m.generateDispatchKey(table, columnValues)
	if err != nil {
		return "", nil, "", errors.Trace(err)
	}

	sql := fmt.Sprintf("delete from `%s`.`%s` where %s limit 1;", schema, table.Name, where)

	return sql, value, key, nil
}

func (m *mysqlTranslator) GenDDLSQL(sql string, schema string) (string, error) {

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
	// if has primary key, use it to construct where condition
	pcs, err := m.pkIndexColumns(table)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	hasPK := (len(pcs) != 0)
	pcsMap := make(map[int64]*model.ColumnInfo)
	for _, col := range pcs {
		pcsMap[col.ID] = col
	}

	var conditionValues []interface{}
	first := true
	for i, col := range columns {
		_, ok := pcsMap[col.ID]
		if !ok && hasPK {
			// if table has primary key, just ignore the non primary key column
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

func (m *mysqlTranslator) genColumnPlaceholders(length int) string {
	values := make([]string, length, length)
	for i := 0; i < length; i++ {
		values[i] = "?"
	}
	return strings.Join(values, ",")
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
		if isPKHandleColumn(table, col) {
			return col
		}
	}

	return nil
}

func (m *mysqlTranslator) pkIndexColumns(table *model.TableInfo) ([]*model.ColumnInfo, error) {
	col := m.pkHandleColumn(table)
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

func isPKHandleColumn(table *model.TableInfo, column *model.ColumnInfo) bool {
	return mysql.HasPriKeyFlag(column.Flag) && table.PKIsHandle
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

func (m *mysqlTranslator) generateDispatchKey(table *model.TableInfo, columnValues map[int64]types.Datum) (string, error) {
	var columnsValues []interface{}
	columns, err := m.pkIndexColumns(table)
	if err != nil {
		return "", errors.Trace(err)
	}
	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if ok {
			value, err := formatData(val, col.FieldType)
			if err != nil {
				return "", errors.Trace(err)
			}
			columnsValues = append(columnsValues, value.GetValue())
		} else {
			columnsValues = append(columnsValues, col.DefaultValue)
		}
	}

	return fmt.Sprintf("%v", columnsValues), nil
}

func formatData(data types.Datum, ft types.FieldType) (types.Datum, error) {
	value, err := tablecodec.Unflatten(data, &ft, time.Local)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if value.GetValue() == nil {
		return value, nil
	}

	switch ft.Tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeDecimal, mysql.TypeNewDecimal:
		value = types.NewDatum(fmt.Sprintf("%v", value.GetValue()))
	case mysql.TypeEnum:
		value = types.NewDatum(value.GetMysqlEnum().Value)
	case mysql.TypeSet:
		value = types.NewDatum(value.GetMysqlSet().Value)
	case mysql.TypeBit:
		value = types.NewDatum(value.GetMysqlBit().Value)
	}

	return value, nil
}

