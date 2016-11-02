package translator

import (
	"bytes"
	"fmt"
	"strings"

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

func (m *mysqlTranslator) GenInsertSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, [][]interface{}, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))

	columnList := m.genColumnList(columns)
	columnPlaceholders := m.genColumnPlaceholders((len(columns)))
	sql := fmt.Sprintf("replace into %s.%s (%s) values (%s);", schema, table.Name, columnList, columnPlaceholders)

	for _, row := range rows {
		//decode the pk value
		remain, pk, err := codec.DecodeOne(row)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		var r []types.Datum
		// decode the remain values, the format is [coldID, colVal, coldID, colVal....]
		if remain[0] != codec.NilFlag {
			r, err = codec.Decode(remain, 2*(len(columns)-1))
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}

		if len(r)%2 != 0 {
			return nil, nil, errors.Errorf("table %s.%s insert row raw data is corruption %v", schema, table.Name, r)
		}

		var columnValues = make(map[int64]types.Datum)
		for i := 0; i < len(r); i += 2 {
			columnValues[r[i].GetInt64()] = r[i+1]
		}

		var vals []interface{}
		for _, col := range columns {
			if m.isPKHandleColumn(table, col) {
				vals = append(vals, pk.GetValue())
				continue
			}

			val, ok := columnValues[col.ID]
			if !ok {
				vals = append(vals, col.DefaultValue)
			} else {

				value, err := m.formatData(val, col.FieldType)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}

				vals = append(vals, value)
			}
		}

		sqls = append(sqls, sql)
		values = append(values, vals)
	}

	return sqls, values, nil
}

func (m *mysqlTranslator) GenUpdateSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, [][]interface{}, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))

	for _, row := range rows {
		var updateColumns []*model.ColumnInfo
		var oldValues []interface{}
		var newValues []interface{}

		// it has pkHandle, get the columm
		pcs, err := m.pkIndexColumns(table)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		// decode one to get the pk
		if pcs != nil {
			remain, _, err := codec.DecodeOne(row)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			row = remain
		}

		// the format
		// 1 have pk index columns: [pk, colID, colVal, colID,..]
		//   the pk index columns' values are constant, we can make up the where condition
		//   from [..., colID, colVal, colID,..] directly
		// 2 no pk index columns: [oldColID, oldColVal, ..., newColID, colVal, ..]
		r, err := codec.Decode(row, 2*(len(columns)-1))
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		if len(r)%2 != 0 {
			return nil, nil, errors.Errorf("table %s.%s update row data is corruption %v", schema, table.Name, r)
		}

		// TODO: if meet old schema that before drop pk index,
		// (now we don't have pk indexs), It can't work well.
		var i int
		columnValues := make(map[int64]types.Datum)
		if pcs == nil {
			for ; i < len(r)/2; i += 2 {
				columnValues[r[i].GetInt64()] = r[i+1]
			}

			updateColumns, oldValues, err = m.generateColumnAndValue(columns, columnValues)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}

			columnValues = make(map[int64]types.Datum)
			for ; i < len(r); i += 2 {
				columnValues[r[i].GetInt64()] = r[i+1]
			}

		} else {
			for ; i < len(r); i += 2 {
				columnValues[r[i].GetInt64()] = r[i+1]
			}

			updateColumns, oldValues, err = m.generateColumnAndValue(pcs, columnValues)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}

		whereColumns := updateColumns
		updateColumns = nil

		updateColumns, newValues, err = m.generateColumnAndValue(columns, columnValues)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		var value []interface{}
		kvs := m.genKVs(updateColumns)
		value = append(value, newValues...)
		value = append(value, oldValues...)

		where := m.genWhere(whereColumns, oldValues)
		sql := fmt.Sprintf("update %s.%s set %s where %s limit 1;", schema, table.Name.L, kvs, where)
		sqls = append(sqls, sql)
		values = append(values, value)
	}

	return sqls, values, nil
}

func (m *mysqlTranslator) GenDeleteSQLsByID(schema string, table *model.TableInfo, rows []int64) ([]string, [][]interface{}, error) {
	sqls := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	column := m.pkHandleColumn(table)
	if column == nil {
		return nil, nil, errors.Errorf("table %s.%s doesn't have pkHandle column", schema, table.Name)
	}
	whereColumns := []*model.ColumnInfo{column}

	for _, rowID := range rows {
		var value []interface{}
		value = append(value, rowID)

		where := m.genWhere(whereColumns, value)
		values = append(values, value)

		sql := fmt.Sprintf("delete from %s.%s where %s limit 1;", schema, table.Name, where)
		sqls = append(sqls, sql)
	}

	return sqls, values, nil
}

func (m *mysqlTranslator) GenDeleteSQLs(schema string, table *model.TableInfo, op OpType, rows [][]byte) ([]string, [][]interface{}, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))

	for _, row := range rows {
		var whereColumns []*model.ColumnInfo
		var value []interface{}
		r, err := codec.Decode(row, len(columns))
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		switch op {
		case DelByPK:
			whereColumns, _ = m.pkIndexColumns(table)
			if whereColumns == nil {
				return nil, nil, errors.Errorf("table %s.%s doesn't have pkHandle column", schema, table.Name)
			}

			if len(r) != len(whereColumns) {
				return nil, nil, errors.Errorf("table %s.%s the delete row by pks binlog %v is courruption", schema, table.Name, r)
			}

			for index, val := range r {
				newValue, err := m.formatData(val, whereColumns[index].FieldType)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}

				value = append(value, newValue)
			}

		case DelByCol:
			if len(r)%2 != 0 {
				return nil, nil, errors.Errorf("table %s.%s the delete row by cols binlog %v is courruption", schema, table.Name, r)
			}

			var columnValues = make(map[int64]types.Datum)
			for i := 0; i < len(r); i += 2 {
				columnValues[r[i].GetInt64()] = r[i+1]
			}

			whereColumns, value, err = m.generateColumnAndValue(columns, columnValues)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}

		default:
			return nil, nil, errors.Errorf("delete row error type %v", op)
		}

		where := m.genWhere(whereColumns, value)
		values = append(values, value)

		sql := fmt.Sprintf("delete from %s.%s where %s limit 1;", schema, table.Name, where)
		sqls = append(sqls, sql)
	}

	return sqls, values, nil
}

func (m *mysqlTranslator) GenDDLSQL(sql string, schema string) (string, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return "", errors.Trace(err)
	}

	_, isCreateDatabase := stmt.(*ast.CreateDatabaseStmt)
	if isCreateDatabase {
		return fmt.Sprintf("%s;", sql), nil
	}

	return fmt.Sprintf("use %s; %s;", schema, sql), nil
}

func (m *mysqlTranslator) genColumnList(columns []*model.ColumnInfo) string {
	var columnList []byte
	for i, column := range columns {
		columnList = append(columnList, []byte(column.Name.L)...)

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
			fmt.Fprintf(&kvs, "%s = ?", columns[i].Name)
		} else {
			fmt.Fprintf(&kvs, "%s = ?, ", columns[i].Name)
		}
	}

	return kvs.String()
}

func (m *mysqlTranslator) genWhere(columns []*model.ColumnInfo, data []interface{}) string {
	var kvs bytes.Buffer
	for i := range columns {
		kvSplit := "="
		if data[i] == nil {
			kvSplit = "is"
		}

		if i == len(columns)-1 {
			fmt.Fprintf(&kvs, "%s %s ?", columns[i].Name, kvSplit)
		} else {
			fmt.Fprintf(&kvs, "%s %s ? and ", columns[i].Name, kvSplit)
		}
	}

	return kvs.String()
}

func (m *mysqlTranslator) pkHandleColumn(table *model.TableInfo) *model.ColumnInfo {
	for _, col := range table.Columns {
		if m.isPKHandleColumn(table, col) {
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
				columns[col.Name.L] = col
			}

			for _, col := range idx.Columns {
				if column, ok := columns[col.Name.L]; ok {
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

func (m *mysqlTranslator) isPKHandleColumn(table *model.TableInfo, column *model.ColumnInfo) bool {
	return mysql.HasPriKeyFlag(column.Flag) && table.PKIsHandle
}

func (m *mysqlTranslator) generateColumnAndValue(columns []*model.ColumnInfo, columnValues map[int64]types.Datum) ([]*model.ColumnInfo, []interface{}, error) {
	var newColumn []*model.ColumnInfo
	var newColumnsValues []interface{}

	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if ok {
			newColumn = append(newColumn, col)
			value, err := m.formatData(val, col.FieldType)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}

			newColumnsValues = append(newColumnsValues, value)
		}
	}

	return newColumn, newColumnsValues, nil
}

func (m *mysqlTranslator) formatData(data types.Datum, ft types.FieldType) (interface{}, error) {
	value, err := tablecodec.Unflatten(data, &ft, false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	switch ft.Tp {
	case mysql.TypeFloat, mysql.TypeTiny, mysql.TypeShort, mysql.TypeYear, mysql.TypeInt24,
		mysql.TypeLong, mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob, mysql.TypeVarchar,
		mysql.TypeString:
		return value.GetValue(), nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeDuration:
		return fmt.Sprintf("%v", value.GetValue()), nil
	case mysql.TypeEnum:
		return value.GetMysqlEnum().Value, nil
	case mysql.TypeSet:
		return value.GetMysqlSet().Value, nil
	case mysql.TypeBit:
		return value.GetMysqlBit().Value, nil
	}

	return value.GetValue(), nil
}
