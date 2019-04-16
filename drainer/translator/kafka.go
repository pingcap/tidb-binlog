package translator

import (
	"fmt"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	parsermysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-binlog/pkg/util"
	obinlog "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
)

// kafkaTranslator translates TiDB binlog to self-description protobuf
type kafkaTranslator struct {
}

func init() {
	Register("kafka", &kafkaTranslator{})
	Register("pulsar", &kafkaTranslator{})
}

func (p *kafkaTranslator) SetConfig(bool, parsermysql.SQLMode) {
	// do nothing
}

func (p *kafkaTranslator) GenInsertSQLs(schema string, tableInfo *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, error) {
	sqls := make([]string, 0, len(rows))
	keys := make([][]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))

	for _, row := range rows {
		table := genTable(schema, tableInfo)
		tableMutation := new(obinlog.TableMutation)
		table.Mutations = append(table.Mutations, tableMutation)
		tableMutation.Type = obinlog.MutationType_Insert.Enum()

		var err error
		tableMutation.Row, err = insertRowToRow(tableInfo, row)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		sqls = append(sqls, "")
		values = append(values, []interface{}{table})
		keys = append(keys, nil)
	}

	return sqls, keys, values, nil
}

func (p *kafkaTranslator) GenUpdateSQLs(schema string, tableInfo *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, bool, error) {
	sqls := make([]string, 0, len(rows))
	keys := make([][]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))

	for _, row := range rows {
		table := genTable(schema, tableInfo)
		tableMutation := new(obinlog.TableMutation)
		table.Mutations = append(table.Mutations, tableMutation)
		tableMutation.Type = obinlog.MutationType_Update.Enum()

		var err error
		tableMutation.Row, tableMutation.ChangeRow, err = updateRowToRow(tableInfo, row)
		if err != nil {
			return nil, nil, nil, false, errors.Trace(err)
		}

		sqls = append(sqls, "")
		values = append(values, []interface{}{table})
		keys = append(keys, nil)
	}

	return sqls, keys, values, false, nil
}

func genTable(schema string, tableInfo *model.TableInfo) (table *obinlog.Table) {
	table = new(obinlog.Table)
	table.SchemaName = proto.String(schema)
	table.TableName = proto.String(tableInfo.Name.O)
	// get obinlog.ColumnInfo
	var columnInfos []*obinlog.ColumnInfo
	for _, col := range tableInfo.Columns {
		info := new(obinlog.ColumnInfo)
		info.Name = col.Name.O
		info.MysqlType = types.TypeToStr(col.Tp, col.Charset)
		if mysql.HasPriKeyFlag(col.Flag) {
			info.IsPrimaryKey = true
		}
		columnInfos = append(columnInfos, info)
	}
	table.ColumnInfo = columnInfos

	return
}

func (p *kafkaTranslator) GenDeleteSQLs(schema string, tableInfo *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, error) {
	sqls := make([]string, 0, len(rows))
	keys := make([][]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))

	for _, row := range rows {
		table := genTable(schema, tableInfo)
		tableMutation := new(obinlog.TableMutation)
		table.Mutations = append(table.Mutations, tableMutation)
		tableMutation.Type = obinlog.MutationType_Delete.Enum()

		var err error
		tableMutation.Row, err = deleteRowToRow(tableInfo, row)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		sqls = append(sqls, "")
		values = append(values, []interface{}{table})
		keys = append(keys, nil)
	}

	return sqls, keys, values, nil
}

func (p *kafkaTranslator) GenDDLSQL(sql string, schema string, commitTS int64) (string, error) {
	return sql, nil
}

func insertRowToRow(tableInfo *model.TableInfo, raw []byte) (row *obinlog.Row, err error) {
	_, columnValues, err := insertRowToDatums(tableInfo, raw)
	columns := tableInfo.Columns

	row = new(obinlog.Row)

	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if !ok {
			val = getDefaultOrZeroValue(col)
		}

		column := DatumToColumn(col, val)
		row.Columns = append(row.Columns, column)
	}

	return
}

func deleteRowToRow(tableInfo *model.TableInfo, raw []byte) (row *obinlog.Row, err error) {
	columns := tableInfo.Columns

	colsTypeMap := util.ToColumnTypeMap(tableInfo.Columns)
	columnValues, err := tablecodec.DecodeRow(raw, colsTypeMap, time.Local)
	if err != nil {
		log.Error(err)
		err = errors.Trace(err)
		return
	}

	// log.Debugf("delete decodeRow: %+v\n", columnValues)

	row = new(obinlog.Row)

	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if !ok {
			val = getDefaultOrZeroValue(col)
		}

		column := DatumToColumn(col, val)
		row.Columns = append(row.Columns, column)
	}

	return
}

func updateRowToRow(tableInfo *model.TableInfo, raw []byte) (row *obinlog.Row, changedRow *obinlog.Row, err error) {
	columns := writableColumns(tableInfo)
	colsTypeMap := util.ToColumnTypeMap(columns)
	oldDatums, newDatums, err := DecodeOldAndNewRow(raw, colsTypeMap, time.Local)
	if err != nil {
		return
	}

	row = new(obinlog.Row)
	changedRow = new(obinlog.Row)
	for _, col := range tableInfo.Columns {
		var val types.Datum
		var ok bool

		if val, ok = newDatums[col.ID]; !ok {
			getDefaultOrZeroValue(col)
		}
		column := DatumToColumn(col, val)
		row.Columns = append(row.Columns, column)

		if val, ok = oldDatums[col.ID]; !ok {
			getDefaultOrZeroValue(col)
		}
		column = DatumToColumn(col, val)
		changedRow.Columns = append(changedRow.Columns, column)
	}

	return
}

// DatumToColumn convert types.Datum to obinlog.Column
func DatumToColumn(colInfo *model.ColumnInfo, datum types.Datum) (col *obinlog.Column) {
	col = new(obinlog.Column)

	if datum.IsNull() {
		col.IsNull = proto.Bool(true)
		return
	}

	switch types.TypeToStr(colInfo.Tp, colInfo.Charset) {
	// date and time type
	case "date", "datetime", "time", "timestamp", "year":
		str := fmt.Sprintf("%v", datum.GetValue())
		col.StringValue = proto.String(str)

	// numeric type
	case "int", "bigint", "smallint", "tinyint":
		str := fmt.Sprintf("%v", datum.GetValue())
		if mysql.HasUnsignedFlag(colInfo.Flag) {
			val, err := strconv.ParseUint(str, 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			col.Uint64Value = proto.Uint64(val)
		} else {
			val, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			col.Int64Value = proto.Int64(val)
		}

	case "float", "double":
		col.DoubleValue = proto.Float64(datum.GetFloat64())
	case "decimal":
		str := fmt.Sprintf("%v", datum.GetValue())
		col.StringValue = proto.String(str)
	case "bit":
		col.BytesValue = datum.GetBytes()

	// string type
	case "text", "longtext", "mediumtext", "char", "tinytext", "varchar", "var_string":
		col.StringValue = proto.String(datum.GetString())
	case "blob", "longblob", "mediumblob", "binary", "tinyblob", "varbinary":
		col.BytesValue = datum.GetBytes()
	case "enum":
		col.Uint64Value = proto.Uint64(datum.GetMysqlEnum().Value)
	case "set":
		col.Uint64Value = proto.Uint64(datum.GetMysqlSet().Value)

	// TiDB don't suppose now
	case "geometry":
		log.Warn("unknown mysql type: ", colInfo.Tp)
		str := fmt.Sprintf("%v", datum.GetValue())
		col.StringValue = proto.String(str)

	case "json":
		col.BytesValue = []byte(datum.GetMysqlJSON().String())

	default:
		log.Warn("unknown mysql type: ", colInfo.Tp)
		str := fmt.Sprintf("%v", datum.GetValue())
		col.StringValue = proto.String(str)

	}

	return
}
