package translator

import (
	"bytes"
	"fmt"
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

func (p *mysqlTranslator) genColumnList(columns []string) string {
	return strings.Join(columns, ",")
}

func (p *mysqlTranslator) TransUpdate(binlog *pb.Binlog, event *pb.Event, row [][]byte) (string, []interface{}, error) {
	// update
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

func genWhere(cols []string, args []interface{}) string {
	var kvs bytes.Buffer
	for i := range cols {
		kvSplit := "="
		if args[i] == nil {
			kvSplit = "IS"
		}
		if i == len(cols)-1 {
			fmt.Fprintf(&kvs, "`%s` %s ?", cols[i], kvSplit)
		} else {
			fmt.Fprintf(&kvs, "`%s` %s ? AND ", cols[i], kvSplit)
		}
	}

	return kvs.String()
}

func (p *mysqlTranslator) TransUpdateSafeMode(binlog *pb.Binlog, event *pb.Event, row [][]byte) (string, []interface{}, error) {
	return "", nil, nil
}

func (p *mysqlTranslator) TransDDL(binlog *pb.Binlog) (string, []interface{}, error) {
	return string(binlog.DdlQuery), nil, nil
}
