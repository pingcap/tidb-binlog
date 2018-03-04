package translator

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/dml"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	tbl "github.com/pingcap/tidb-binlog/restore/table"
	"github.com/pingcap/tidb/util/codec"
)

type mysqlTranslator struct{}

func newMysqlTranslator() Translator {
	return &mysqlTranslator{}
}

func (p *mysqlTranslator) TransInsert(binlog *pb.Binlog, event *pb.Event, row [][]byte, table *tbl.Table) (*TranslateResult, error) {
	schemaName := *event.SchemaName
	tableName := *event.TableName
	placeholders := dml.GenColumnPlaceholders(len(row))

	cols, args, err := genColsAndArgs(row)
	if err != nil {
		return nil, errors.Trace(err)
	}

	keys := genMultipleKeys(table.Columns, args, table.IndexColumns)

	columnList := p.genColumnList(cols)
	sql := fmt.Sprintf("REPLACE INTO `%s`.`%s` (%s) VALUES (%s);", schemaName, tableName, columnList, placeholders)

	log.Debugf("insert sql %s, args %+v, keys %+v", sql, args, keys)
	result := &TranslateResult{
		SQL:  sql,
		Keys: keys,
		Args: args,
	}
	return result, nil
}

func (p *mysqlTranslator) TransUpdate(binlog *pb.Binlog, event *pb.Event, row [][]byte, table *tbl.Table) (*TranslateResult, error) {
	schemaName := *event.SchemaName
	tableName := *event.TableName
	allCols := make([]string, 0, len(row))
	oldValues := make([]interface{}, 0, len(row))
	changedValues := make([]interface{}, 0, len(row))

	updatedColumns := make([]string, 0, len(row))
	updatedValues := make([]interface{}, 0, len(row))
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

		log.Debugf("%s(%s %v): %v => %v\n", col.Name, col.MysqlType, tp, oldDatum.GetValue(), changedDatum.GetValue())

		if reflect.DeepEqual(oldDatum.GetValue(), changedDatum.GetValue()) {
			continue
		}
		updatedColumns = append(updatedColumns, col.Name)
		updatedValues = append(updatedValues, changedDatum.GetValue())
		// fmt.Printf("%s(%s): %s => %s\n", col.Name, col.MysqlType, formatValueToString(val, tp), formatValueToString(changedVal, tp))
	}

	kvs := genKVs(updatedColumns)
	where := genWhere(allCols, oldValues)

	args := make([]interface{}, 0, len(updatedValues)+len(oldValues))
	args = append(args, updatedValues...)
	args = append(args, oldValues...)

	//TODO
	var keys []string
	sql := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s LIMIT 1;", schemaName, tableName, kvs, where)
	log.Debugf("update sql %s, args %+v", sql, args)
	result := &TranslateResult{
		SQL:  sql,
		Keys: keys,
		Args: args,
	}
	return result, nil
}

func (p *mysqlTranslator) TransUpdateSafeMode(binlog *pb.Binlog, event *pb.Event, row [][]byte, table *tbl.Table) (*TranslateResult, error) {
	return nil, nil
}

func (p *mysqlTranslator) TransDelete(binlog *pb.Binlog, event *pb.Event, row [][]byte, table *tbl.Table) (*TranslateResult, error) {
	schemaName := *event.SchemaName
	tableName := *event.TableName

	cols, args, err := genColsAndArgs(row)
	if err != nil {
		return nil, errors.Trace(err)
	}

	where := genWhere(cols, args)

	//TODO
	var keys []string
	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s limit 1", schemaName, tableName, where)
	log.Debugf("delete sql %s, args %+v", sql, args)
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

func (p *mysqlTranslator) TransDDL(binlog *pb.Binlog) (*TranslateResult, error) {
	return &TranslateResult{SQL: string(binlog.DdlQuery)}, nil
}

func (p *mysqlTranslator) genColumnList(columns []string) string {
	return strings.Join(columns, ",")
}

func genWhere(columns []string, args []interface{}) string {
	items := make([]string, 0, len(columns))
	for i, col := range columns {
		kvSplit := "="
		if args[i] == nil {
			kvSplit = "IS"
		}
		item := fmt.Sprintf("`%s` %s ?", col, kvSplit)
		items = append(items, item)
	}

	return strings.Join(items, " AND ")
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

func genMultipleKeys(columns []*tbl.Column, value []interface{}, indexColumns map[string][]*tbl.Column) []string {
	var multipleKeys []string
	for _, indexCols := range indexColumns {
		cols, vals := getColumnData(columns, indexCols, value)
		multipleKeys = append(multipleKeys, genKeyList(cols, vals))
	}
	return multipleKeys
}

func genKeyList(columns []*tbl.Column, dataSeq []interface{}) string {
	values := make([]string, 0, len(dataSeq))
	for i, data := range dataSeq {
		values = append(values, columnValue(data, columns[i].Unsigned))
	}

	return strings.Join(values, ",")
}

func getColumnData(columns []*tbl.Column, indexColumns []*tbl.Column, data []interface{}) ([]*tbl.Column, []interface{}) {
	cols := make([]*tbl.Column, 0, len(columns))
	values := make([]interface{}, 0, len(columns))
	for _, column := range indexColumns {
		cols = append(cols, column)
		values = append(values, data[column.Idx])
	}

	return cols, values
}

func findFitIndex(indexColumns map[string][]*tbl.Column) []*tbl.Column {
	cols, ok := indexColumns["primary"]
	if ok {
		if len(cols) == 0 {
			log.Error("cols is empty")
		} else {
			return cols
		}
	}

	// second find not null unique key
	fn := func(c *tbl.Column) bool {
		return !c.NotNull
	}

	return getSpecifiedIndexColumn(indexColumns, fn)
}

func getAvailableIndexColumn(indexColumns map[string][]*tbl.Column, data []interface{}) []*tbl.Column {
	fn := func(c *tbl.Column) bool {
		return data[c.Idx] == nil
	}

	return getSpecifiedIndexColumn(indexColumns, fn)
}

func getSpecifiedIndexColumn(indexColumns map[string][]*tbl.Column, fn func(col *tbl.Column) bool) []*tbl.Column {
	for _, indexCols := range indexColumns {
		if len(indexCols) == 0 {
			continue
		}

		findFitIndex := true
		for _, col := range indexCols {
			if fn(col) {
				findFitIndex = false
				break
			}
		}

		if findFitIndex {
			return indexCols
		}
	}

	return nil
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
