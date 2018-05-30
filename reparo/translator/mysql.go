package translator

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/dml"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb-binlog/reparo/common"
	"github.com/pingcap/tidb/util/codec"
)

type mysqlTranslator struct {
	db     *sql.DB
	tables map[string]*common.Table
}

func newMysqlTranslator(cfg *common.DBConfig) (Translator, error) {
	db, err := pkgsql.OpenDB("mysql", cfg.Host, cfg.Port, cfg.User, cfg.Password)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &mysqlTranslator{
		db:     db,
		tables: make(map[string]*common.Table),
	}, nil
}

func (m *mysqlTranslator) TransInsert(event *pb.Event) (*TranslateResult, error) {
	schemaName := *event.SchemaName
	tableName := *event.TableName
	row := event.GetRow()
	table, err := m.getTable(schemaName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	placeholders := dml.GenColumnPlaceholders(len(row))

	cols, args, err := genColsAndArgs(row)
	if err != nil {
		return nil, errors.Trace(err)
	}

	keys := genMultipleKeys(table.Columns, args, table.IndexColumns)

	columnList := m.genColumnList(cols)
	sql := fmt.Sprintf("REPLACE INTO `%s`.`%s` (%s) VALUES (%s);", schemaName, tableName, columnList, placeholders)

	log.Debugf("insert sql %s, args %+v, keys %+v", sql, args, keys)
	result := &TranslateResult{
		SQL:  sql,
		Keys: keys,
		Args: args,
	}
	return result, nil
}

func (m *mysqlTranslator) TransUpdate(event *pb.Event) (*TranslateResult, error) {
	schemaName := *event.SchemaName
	tableName := *event.TableName
	row := event.GetRow()
	table, err := m.getTable(schemaName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	allCols := make([]string, 0, len(row))
	oldValues := make([]interface{}, 0, len(row))

	updatedColumns := make([]string, 0, len(row))
	// updatedValues is the values which is really updated.
	updatedValues := make([]interface{}, 0, len(row))
	// changedValues is the values which is just recorded by the binlog event(whatever it's updated or not).
	changedValues := make([]interface{}, 0, len(row))
	for _, c := range row {
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			return nil, errors.Trace(err)
		}
		allCols = append(allCols, col.Name)

		_, oldValue, err := codec.DecodeOne(col.Value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		_, changedValue, err := codec.DecodeOne(col.ChangedValue)
		if err != nil {
			return nil, errors.Trace(err)
		}

		tp := col.Tp[0]
		oldDatum := formatValue(oldValue, tp)
		oldValues = append(oldValues, oldDatum.GetValue())
		changedDatum := formatValue(changedValue, tp)
		changedValues = append(changedValues, changedDatum.GetValue())

		log.Debugf("update value %s(%s %v): %v => %v\n", col.Name, col.MysqlType, tp, oldDatum.GetValue(), changedDatum.GetValue())

		if reflect.DeepEqual(oldDatum.GetValue(), changedDatum.GetValue()) {
			continue
		}
		updatedColumns = append(updatedColumns, col.Name)
		updatedValues = append(updatedValues, changedDatum.GetValue())
	}

	keys := genMultipleKeys(table.Columns, oldValues, table.IndexColumns)
	keys = append(keys, genMultipleKeys(table.Columns, changedValues, table.IndexColumns)...)

	if len(updatedColumns) == 0 {
		log.Warnf("%s.%s updated columns is empty", schemaName, tableName)
		return nil, nil
	}

	kvs := genKVs(updatedColumns)
	where, oldValues := genWhere(allCols, oldValues)

	args := make([]interface{}, 0, len(updatedValues)+len(oldValues))
	args = append(args, updatedValues...)
	args = append(args, oldValues...)

	sql := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s LIMIT 1;", schemaName, tableName, kvs, where)
	log.Debugf("update sql %s, args %+v, keys %+v", sql, args, keys)
	result := &TranslateResult{
		SQL:  sql,
		Keys: keys,
		Args: args,
	}
	return result, nil
}

func (m *mysqlTranslator) TransDelete(event *pb.Event) (*TranslateResult, error) {
	schemaName := *event.SchemaName
	tableName := *event.TableName
	row := event.GetRow()
	table, err := m.getTable(schemaName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cols, args, err := genColsAndArgs(row)
	if err != nil {
		return nil, errors.Trace(err)
	}
	keys := genMultipleKeys(table.Columns, args, table.IndexColumns)
	where, args := genWhere(cols, args)

	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s limit 1", schemaName, tableName, where)
	log.Debugf("delete sql %s, args %+v, keys %+v", sql, args, keys)
	result := &TranslateResult{
		SQL:  sql,
		Keys: keys,
		Args: args,
	}
	return result, nil
}

func genColsAndArgs(row [][]byte) ([]string, []interface{}, error) {
	cols := make([]string, 0, len(row))
	args := make([]interface{}, 0, len(row))
	for _, c := range row {
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		cols = append(cols, col.Name)

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		tp := col.Tp[0]
		val = formatValue(val, tp)
		log.Debugf("%s(%s): %v \n", col.Name, col.MysqlType, val.GetValue())
		args = append(args, val.GetValue())
	}
	return cols, args, nil
}

func (m *mysqlTranslator) TransDDL(ddl string) (*TranslateResult, error) {
	m.clearTables()
	return &TranslateResult{SQL: ddl}, nil
}

func (m *mysqlTranslator) Close() error {
	return m.db.Close()
}

func (m *mysqlTranslator) genColumnList(columns []string) string {
	return strings.Join(columns, ",")
}

func genWhere(columns []string, args []interface{}) (string, []interface{}) {
	items := make([]string, 0, len(columns))
	var conditionValues []interface{}
	for i, col := range columns {
		kvSplit := "= ?"
		if args[i] == nil {
			kvSplit = "IS NULL"
		} else {
			conditionValues = append(conditionValues, args[i])
		}

		item := fmt.Sprintf("`%s` %s", col, kvSplit)
		items = append(items, item)
	}

	return strings.Join(items, " AND "), conditionValues
}

func genKVs(columns []string) string {
	var kvs bytes.Buffer
	for i := range columns {
		if i == len(columns)-1 {
			fmt.Fprintf(&kvs, "`%s` = ?", columns[i])
		} else {
			fmt.Fprintf(&kvs, "`%s` = ?, ", columns[i])
		}
	}
	return kvs.String()
}

func genMultipleKeys(columns []*common.Column, value []interface{}, indexColumns map[string][]*common.Column) []string {
	var multipleKeys []string
	for _, indexCols := range indexColumns {
		cols, vals := getColumnData(columns, indexCols, value)
		multipleKeys = append(multipleKeys, genKeyList(cols, vals))
	}
	return multipleKeys
}

func genKeyList(columns []*common.Column, dataSeq []interface{}) string {
	values := make([]string, 0, len(dataSeq))
	for i, data := range dataSeq {
		values = append(values, columnValue(data, columns[i].Unsigned))
	}

	return strings.Join(values, ",")
}

func getColumnData(columns []*common.Column, indexColumns []*common.Column, data []interface{}) ([]*common.Column, []interface{}) {
	cols := make([]*common.Column, 0, len(columns))
	values := make([]interface{}, 0, len(columns))
	for _, column := range indexColumns {
		cols = append(cols, column)
		values = append(values, data[column.Idx])
	}

	return cols, values
}

func castUnsigned(data interface{}, unsigned bool) interface{} {
	if !unsigned {
		return data
	}

	switch v := data.(type) {
	case int:
		return uint(v)
	case int8:
		return uint8(v)
	case int16:
		return uint16(v)
	case int32:
		return uint32(v)
	case int64:
		return strconv.FormatUint(uint64(v), 10)
	}

	return data
}

func columnValue(value interface{}, unsigned bool) string {
	castValue := castUnsigned(value, unsigned)

	var data string
	switch v := castValue.(type) {
	case nil:
		data = "null"
	case bool:
		if v {
			data = "1"
		} else {
			data = "0"
		}
	case int:
		data = strconv.FormatInt(int64(v), 10)
	case int8:
		data = strconv.FormatInt(int64(v), 10)
	case int16:
		data = strconv.FormatInt(int64(v), 10)
	case int32:
		data = strconv.FormatInt(int64(v), 10)
	case int64:
		data = strconv.FormatInt(int64(v), 10)
	case uint8:
		data = strconv.FormatUint(uint64(v), 10)
	case uint16:
		data = strconv.FormatUint(uint64(v), 10)
	case uint32:
		data = strconv.FormatUint(uint64(v), 10)
	case uint64:
		data = strconv.FormatUint(uint64(v), 10)
	case float32:
		data = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		data = strconv.FormatFloat(float64(v), 'f', -1, 64)
	case string:
		data = v
	case []byte:
		data = string(v)
	default:
		data = fmt.Sprintf("%v", v)
	}

	return data
}
