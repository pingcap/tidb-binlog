package translator

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/dml"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/util/codec"
)

type mysqlTranslator struct {
}

func newMysqlTranslator() Translator {
	return &mysqlTranslator{}
}

func (p *mysqlTranslator) TransInsert(binlog *pb.Binlog, event *pb.Event, row [][]byte) (string, []interface{}, error) {
	cols := make([]string, 0, len(row))
	args := make([]interface{}, 0, len(row))
	schema := *event.SchemaName
	table := *event.TableName
	placeholders := dml.GenColumnPlaceholders(len(row))

	for _, c := range row {
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			return "", nil, errors.Trace(err)
		}
		cols = append(cols, col.Name)

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			return "", nil, errors.Trace(err)
		}

		tp := col.Tp[0]
		val = formatValue(val, tp)
		log.Debugf("%s(%s): %s \n", col.Name, col.MysqlType, formatValueToString(val, tp))
		args = append(args, val.GetValue())
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
	changedValues := make([]interface{}, 0, len(row))

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
		changedValues = append(changedValues, changedDatum.GetValue())

		log.Debugf("%s(%s): %s => %s\n", col.Name, col.MysqlType, formatValueToString(oldDatum, tp), formatValueToString(changedDatum, tp))

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

	sql := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s LIMIT 1;", schema, table, kvs, where)
	log.Debugf("update sql %s, args %+v", sql, args)
	return sql, args, nil
}

func (p *mysqlTranslator) TransUpdateSafeMode(binlog *pb.Binlog, event *pb.Event, row [][]byte) (string, []interface{}, error) {
	return "", nil, nil
}

func (p *mysqlTranslator) TransDelete(binlog *pb.Binlog, event *pb.Event, row [][]byte) (string, []interface{}, error) {
	cols := make([]string, 0, len(row))
	args := make([]interface{}, 0, len(row))
	schema := *event.SchemaName
	table := *event.TableName

	for _, c := range row {
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			return "", nil, errors.Trace(err)
		}
		cols = append(cols, col.Name)

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			return "", nil, errors.Trace(err)
		}

		tp := col.Tp[0]
		val = formatValue(val, tp)
		log.Debugf("%s(%s): %s \n", col.Name, col.MysqlType, formatValueToString(val, tp))
		args = append(args, val.GetValue())
	}

	where := genWhere(cols, args)
	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s limit 1", schema, table, where)
	log.Debugf("delete sql %s, args %+v", sql, args)
	return sql, args, nil
}

func (p *mysqlTranslator) TransDDL(binlog *pb.Binlog) (string, []interface{}, error) {
	return string(binlog.DdlQuery), nil, nil
}

func (p *mysqlTranslator) genColumnList(columns []string) string {
	return strings.Join(columns, ",")
}

func genWhere(columns []string, args []interface{}) string {
	var kvs bytes.Buffer
	for i := range columns {
		kvSplit := "="
		if args[i] == nil {
			kvSplit = "IS"
		}
		if i == len(columns)-1 {
			fmt.Fprintf(&kvs, "`%s` %s ?", columns[i], kvSplit)
		} else {
			fmt.Fprintf(&kvs, "`%s` %s ? AND ", columns[i], kvSplit)
		}
	}

	return kvs.String()
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
