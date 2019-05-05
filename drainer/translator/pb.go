package translator

import (
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-binlog/pkg/util"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

// pbTranslator translates TiDB binlog to self-description protobuf
type pbTranslator struct {
	sqlMode mysql.SQLMode
}

func init() {
	Register("file", &pbTranslator{})
}

func (p *pbTranslator) SetConfig(_ bool, sqlMode mysql.SQLMode) {
	p.sqlMode = sqlMode
}

func (p *pbTranslator) GenInsertSQLs(schema string, table *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	keys := make([][]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))

	for _, row := range rows {
		_, columnValues, err := insertRowToDatums(table, row)
		if err != nil {
			return nil, nil, nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name)
		}

		var (
			vals       = make([]types.Datum, 0, len(columns))
			cols       = make([]string, 0, len(columns))
			tps        = make([]byte, 0, len(columns))
			mysqlTypes = make([]string, 0, len(columns))
		)
		for _, col := range columns {
			cols = append(cols, col.Name.O)
			tps = append(tps, col.Tp)
			mysqlTypes = append(mysqlTypes, types.TypeToStr(col.Tp, col.Charset))
			val, ok := columnValues[col.ID]
			if !ok {
				val = getDefaultOrZeroValue(col)
			}

			value, err := formatData(val, col.FieldType)
			if err != nil {
				return nil, nil, nil, errors.Trace(err)
			}
			vals = append(vals, value)
		}

		rowData, err := encodeRow(vals, cols, tps, mysqlTypes)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		sqls = append(sqls, "")
		values = append(values, packEvent(schema, table.Name.O, pb.EventType_Insert, rowData))
		keys = append(keys, nil)
	}

	return sqls, keys, values, nil
}

func (p *pbTranslator) GenUpdateSQLs(schema string, table *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, bool, error) {
	columns := writableColumns(table)
	sqls := make([]string, 0, len(rows))
	keys := make([][]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	colsTypeMap := util.ToColumnTypeMap(columns)

	for _, row := range rows {
		oldColumnValues, newColumnValues, err := DecodeOldAndNewRow(row, colsTypeMap, time.Local)
		if err != nil {
			return nil, nil, nil, false, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name)
		}

		if len(newColumnValues) == 0 {
			continue
		}

		var (
			oldVals    = make([]types.Datum, 0, len(columns))
			newVals    = make([]types.Datum, 0, len(columns))
			cols       = make([]string, 0, len(columns))
			tps        = make([]byte, 0, len(columns))
			mysqlTypes = make([]string, 0, len(columns))
		)
		for _, col := range columns {
			val, ok := newColumnValues[col.ID]
			if ok {
				oldValue, err := formatData(oldColumnValues[col.ID], col.FieldType)
				if err != nil {
					return nil, nil, nil, false, errors.Trace(err)
				}
				newValue, err := formatData(val, col.FieldType)
				if err != nil {
					return nil, nil, nil, false, errors.Trace(err)
				}
				oldVals = append(oldVals, oldValue)
				newVals = append(newVals, newValue)
				cols = append(cols, col.Name.O)
				tps = append(tps, col.Tp)
				mysqlTypes = append(mysqlTypes, types.TypeToStr(col.Tp, col.Charset))
			}
		}

		rowData, err := encodeUpdateRow(oldVals, newVals, cols, tps, mysqlTypes)
		if err != nil {
			return nil, nil, nil, false, errors.Trace(err)
		}

		sqls = append(sqls, "")
		values = append(values, packEvent(schema, table.Name.O, pb.EventType_Update, rowData))
		keys = append(keys, nil)
	}

	return sqls, keys, values, false, nil
}

func (p *pbTranslator) GenDeleteSQLs(schema string, table *model.TableInfo, rows [][]byte, commitTS int64) ([]string, [][]string, [][]interface{}, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	keys := make([][]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	colsTypeMap := util.ToColumnTypeMap(columns)

	for _, row := range rows {
		columnValues, err := tablecodec.DecodeRow(row, colsTypeMap, time.Local)
		if err != nil {
			return nil, nil, nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name)
		}
		if columnValues == nil {
			continue
		}

		var (
			vals       = make([]types.Datum, 0, len(columns))
			cols       = make([]string, 0, len(columns))
			tps        = make([]byte, 0, len(columns))
			mysqlTypes = make([]string, 0, len(columns))
		)
		for _, col := range columns {
			val, ok := columnValues[col.ID]
			if ok {
				value, err := formatData(val, col.FieldType)
				if err != nil {
					return nil, nil, nil, errors.Trace(err)
				}
				vals = append(vals, value)
				cols = append(cols, col.Name.O)
				tps = append(tps, col.Tp)
				mysqlTypes = append(mysqlTypes, types.TypeToStr(col.Tp, col.Charset))
			}
		}

		rowData, err := encodeRow(vals, cols, tps, mysqlTypes)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		sqls = append(sqls, "")
		values = append(values, packEvent(schema, table.Name.O, pb.EventType_Delete, rowData))
		keys = append(keys, nil)
	}

	return sqls, keys, values, nil
}

func (p *pbTranslator) GenDDLSQL(sql string, schema string, commitTS int64) (string, error) {
	ddlParser := parser.New()
	ddlParser.SetSQLMode(p.sqlMode)
	stmt, err := ddlParser.ParseOneStmt(sql, "", "")
	if err != nil {
		return "", errors.Trace(err)
	}

	_, isCreateDatabase := stmt.(*ast.CreateDatabaseStmt)
	if isCreateDatabase {
		return fmt.Sprintf("%s;", sql), nil
	}

	return fmt.Sprintf("use %s; %s;", schema, sql), nil
}

func encodeRow(row []types.Datum, colName []string, tp []byte, mysqlType []string) ([][]byte, error) {
	var cols [][]byte
	var err error
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for i, c := range row {
		col := &pb.Column{}
		col.Name = colName[i]
		col.Tp = []byte{tp[i]}
		col.MysqlType = mysqlType[i]
		col.Value, err = codec.EncodeValue(sc, nil, []types.Datum{c}...)
		if err != nil {
			return nil, errors.Trace(err)
		}

		colVal, err := col.Marshal()
		if err != nil {
			return nil, errors.Trace(err)
		}
		cols = append(cols, colVal)
	}

	return cols, nil
}

func encodeUpdateRow(oldRow []types.Datum, newRow []types.Datum, colName []string, tp []byte, mysqlType []string) ([][]byte, error) {
	var cols [][]byte
	var err error
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for i, c := range oldRow {
		col := &pb.Column{}
		col.Name = colName[i]
		col.Tp = []byte{tp[i]}
		col.MysqlType = mysqlType[i]
		col.Value, err = codec.EncodeValue(sc, nil, []types.Datum{c}...)
		if err != nil {
			return nil, errors.Trace(err)
		}

		col.ChangedValue, err = codec.EncodeValue(sc, nil, []types.Datum{newRow[i]}...)
		if err != nil {
			return nil, errors.Trace(err)
		}

		colVal, err := col.Marshal()
		if err != nil {
			return nil, errors.Trace(err)
		}
		cols = append(cols, colVal)
	}

	return cols, nil
}

func packEvent(schemaName, tableName string, tp pb.EventType, rowData [][]byte) []interface{} {
	event := &pb.Event{
		SchemaName: proto.String(schemaName),
		TableName:  proto.String(tableName),
		Row:        rowData,
		Tp:         tp,
	}

	return []interface{}{event}
}
