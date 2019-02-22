package translator

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/dml"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/util/codec"
)

type mysqlTranslator struct{}

func newMysqlTranslator() Translator {
	return &mysqlTranslator{}
}

func (p *mysqlTranslator) TransInsert(binlog *pb.Binlog, event *pb.Event, row [][]byte) (string, []interface{}, error) {
	schema := *event.SchemaName
	table := *event.TableName
	placeholders := dml.GenColumnPlaceholders(len(row))

	cols, args, err := genColsAndArgs(row)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	columnList := p.genColumnList(cols)
	sql := fmt.Sprintf("REPLACE INTO `%s`.`%s` (%s) VALUES (%s);", schema, table, columnList, placeholders)

	log.Debugf("insert sql %s, args %+v", sql, args)
	return sql, args, nil
}

func (p *mysqlTranslator) TransUpdate(binlog *pb.Binlog, event *pb.Event, row [][]byte) (string, []interface{}, error) {
	schema := *event.SchemaName
	table := *event.TableName
	allCols := make([]string, 0, len(row))
	oldValues := make([]interface{}, 0, len(row))

	updatedColumns := make([]string, 0, len(row))
	updatedValues := make([]interface{}, 0, len(row))
	for _, c := range row {
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			return "", nil, errors.Trace(err)
		}
		allCols = append(allCols, col.Name)

		_, oldValue, err := codec.DecodeOne(col.Value)
		if err != nil {
			return "", nil, errors.Trace(err)
		}
		_, changedValue, err := codec.DecodeOne(col.ChangedValue)
		if err != nil {
			return "", nil, errors.Trace(err)
		}

		tp := col.Tp[0]
		oldDatum := formatValue(oldValue, tp)
		oldValues = append(oldValues, oldDatum.GetValue())
		changedDatum := formatValue(changedValue, tp)

		log.Debugf("%s(%s %v): %v => %v\n", col.Name, col.MysqlType, tp, oldDatum.GetValue(), changedDatum.GetValue())

		if reflect.DeepEqual(oldDatum.GetValue(), changedDatum.GetValue()) {
			continue
		}
		updatedColumns = append(updatedColumns, col.Name)
		updatedValues = append(updatedValues, changedDatum.GetValue())
		// fmt.Printf("%s(%s): %s => %s\n", col.Name, col.MysqlType, formatValueToString(val, tp), formatValueToString(changedVal, tp))
	}

	kvs := genKVs(updatedColumns)
	where, oldValues := genWhere(allCols, oldValues)

	args := make([]interface{}, 0, len(updatedValues)+len(oldValues))
	args = append(args, updatedValues...)
	args = append(args, oldValues...)

	sql := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s LIMIT 1;", schema, table, kvs, where)
	log.Debugf("update sql %s, args %+v", sql, args)
	return sql, args, nil
}

func (p *mysqlTranslator) TransDelete(binlog *pb.Binlog, event *pb.Event, row [][]byte) (string, []interface{}, error) {
	schema := *event.SchemaName
	table := *event.TableName

	cols, args, err := genColsAndArgs(row)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	where, args := genWhere(cols, args)
	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s limit 1", schema, table, where)
	log.Debugf("delete sql %s, args %+v", sql, args)
	return sql, args, nil
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

func (p *mysqlTranslator) TransDDL(binlog *pb.Binlog) (string, []interface{}, error) {
	return string(binlog.DdlQuery), nil, nil
}

func (p *mysqlTranslator) genColumnList(columns []string) string {
	var columnList []byte
	for i, column := range columns {
		name := fmt.Sprintf("`%s`", column)
		columnList = append(columnList, []byte(name)...)

		if i != len(columns)-1 {
			columnList = append(columnList, ',')
		}
	}

	return string(columnList)
}

func genWhere(columns []string, args []interface{}) (string, []interface{}) {
	items := make([]string, 0, len(columns))
	newArgs := make([]interface{}, 0, len(args))
	var item string

	for i, col := range columns {
		if args[i] == nil {
			item = fmt.Sprintf("`%s` IS NULL", col)
		} else {
			item = fmt.Sprintf("`%s` = ?", col)
			newArgs = append(newArgs, args[i])
		}
		items = append(items, item)
	}

	return strings.Join(items, " AND "), newArgs
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
