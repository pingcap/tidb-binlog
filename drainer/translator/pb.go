package translator

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

// pbTranslator translates TiDB binlog to self-description protobuf
type pbTranslator struct{}

func init() {
	Register("pb", &pbTranslator{})
}

func (p *pbTranslator) GenInsertSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, []string, [][]interface{}, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	keys := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		mutation := &pb.Event{}
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
			return nil, nil, nil, errors.Errorf("table %s.%s insert row raw data is corruption %v", schema, table.Name, r)
		}

		var columnValues = make(map[int64]types.Datum)
		for i := 0; i < len(r); i += 2 {
			columnValues[r[i].GetInt64()] = r[i+1]
		}

		var (
			vals = make([]types.Datum, 0, len(columns))
			cols = make([]string, 0, len(columns))
			tps  = make([]byte, 0, len(columns))
		)
		for _, col := range columns {
			if isPKHandleColumn(table, col) {
				columnValues[col.ID] = pk
			}

			val, ok := columnValues[col.ID]
			if ok {
				value, err := tablecodec.Unflatten(val, &col.FieldType, false)
				if err != nil {
					return nil, nil, nil, errors.Trace(err)
				}
				vals = append(vals, value)
				cols = append(cols, col.Name.L)
				tps = append(tps, col.Tp)
			}
		}

		rowVal, err := encodeRow(vals, cols, tps)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		mutation.Row = rowVal
		mutation.SchemaName = proto.String(schema)
		mutation.TableName = proto.String(table.Name.L)
		mutation.Tp = pb.EventType_Insert

		sqls = append(sqls, "")
		values = append(values, []interface{}{mutation})
		keys = append(keys, "")
	}

	return sqls, keys, values, nil
}

func (p *pbTranslator) GenUpdateSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, []string, [][]interface{}, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	keys := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		mutation := &pb.Event{}
		r, err := codec.Decode(row, 2*len(columns))
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		if len(r)%2 != 0 {
			return nil, nil, nil, errors.Errorf("table %s.%s update row data is corruption %v", schema, table.Name, r)
		}

		var i int
		// old column values
		oldColumnValues := make(map[int64]types.Datum)
		for ; i < len(r)/2; i += 2 {
			oldColumnValues[r[i].GetInt64()] = r[i+1]
		}
		// new coulmn values
		newColumnVlaues := make(map[int64]types.Datum)
		for ; i < len(r); i += 2 {
			newColumnVlaues[r[i].GetInt64()] = r[i+1]
		}

		var (
			oldVals = make([]types.Datum, 0, len(columns))
			newVals = make([]types.Datum, 0, len(columns))
			cols    = make([]string, 0, len(columns))
			tps     = make([]byte, 0, len(columns))
		)
		for _, col := range columns {
			val, ok := newColumnVlaues[col.ID]
			if ok {
				newValue, err := tablecodec.Unflatten(val, &col.FieldType, false)
				if err != nil {
					return nil, nil, nil, errors.Trace(err)
				}
				oldValue, err := tablecodec.Unflatten(oldColumnValues[col.ID], &col.FieldType, false)
				if err != nil {
					return nil, nil, nil, errors.Trace(err)
				}
				oldVals = append(oldVals, oldValue)
				newVals = append(newVals, newValue)
				cols = append(cols, col.Name.L)
				tps = append(tps, col.Tp)
			}
		}

		rowVal, err := encodeUpdateRow(oldVals, newVals, cols, tps)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		mutation.Row = rowVal
		mutation.SchemaName = proto.String(schema)
		mutation.TableName = proto.String(table.Name.L)
		mutation.Tp = pb.EventType_Update
		sqls = append(sqls, "")
		values = append(values, []interface{}{mutation})
		keys = append(keys, "")
	}

	return sqls, keys, values, nil
}

func (p *pbTranslator) GenDeleteSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, []string, [][]interface{}, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	keys := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))

	for _, row := range rows {
		mutation := &pb.Event{}
		r, err := codec.Decode(row, len(columns))
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		if len(r)%2 != 0 {
			return nil, nil, nil, errors.Errorf("table %s.%s the delete row by cols binlog %v is courruption", schema, table.Name, r)
		}

		var columnValues = make(map[int64]types.Datum)
		for i := 0; i < len(r); i += 2 {
			columnValues[r[i].GetInt64()] = r[i+1]
		}

		var (
			vals = make([]types.Datum, 0, len(columns))
			cols = make([]string, 0, len(columns))
			tps  = make([]byte, 0, len(columns))
		)
		for _, col := range columns {
			val, ok := columnValues[col.ID]
			if ok {
				value, err := tablecodec.Unflatten(val, &col.FieldType, false)
				if err != nil {
					return nil, nil, nil, errors.Trace(err)
				}
				vals = append(vals, value)
				cols = append(cols, col.Name.L)
				tps = append(tps, col.Tp)
			}
		}

		rowVal, err := encodeRow(vals, cols, tps)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		mutation.Row = rowVal
		mutation.SchemaName = proto.String(schema)
		mutation.TableName = proto.String(table.Name.L)
		mutation.Tp = pb.EventType_Delete
		sqls = append(sqls, "")
		values = append(values, []interface{}{mutation})
		keys = append(keys, "")
	}

	return sqls, keys, values, nil
}

func (p *pbTranslator) GenDDLSQL(sql string, schema string) (string, error) {
	stmts, err := parser.New().Parse(sql, "", "")
	if err != nil {
		return "", errors.Trace(err)
	}

	stmt := stmts[0]
	_, isCreateDatabase := stmt.(*ast.CreateDatabaseStmt)
	if isCreateDatabase {
		return fmt.Sprintf("%s;", sql), nil
	}

	return fmt.Sprintf("use %s; %s;", schema, sql), nil
}

// encodeRow encode insert/delete column name, file tp and row data into a slice of byte.
// Row layout: colName1, tp1, value1, colName2, tp2, value2, .....
func encodeRow(row []types.Datum, colName []string, tp []byte) ([]byte, error) {
	if len(row) != len(colName) || len(row) != len(tp) {
		return nil, errors.Errorf("EncodeRow error: data, columnName and tp count not match %d vs %d vs %d", len(row), len(colName), len(tp))
	}
	values := make([]types.Datum, 3*len(row))
	for i, c := range row {
		values[3*i] = types.NewStringDatum(colName[i])
		values[3*i+1] = types.NewBytesDatum([]byte{tp[i]})
		values[3*i+2] = c
	}
	if len(values) == 0 {
		// We could not set nil value into kv.
		return []byte{codec.NilFlag}, nil
	}
	return codec.EncodeValue(nil, values...)
}

// encodeUpdateRow encode insert/delete column name, file tp and old/new row data into a slice of byte.
// Row layout: colName1, tp1, oldValue1, newValue1, colName2, tp2, oldValue2, newValue2, .....
func encodeUpdateRow(oldRow []types.Datum, newRow []types.Datum, colName []string, tp []byte) ([]byte, error) {
	if len(oldRow) != len(colName) || len(oldRow) != len(newRow) || len(oldRow) != len(tp) {
		return nil, errors.Errorf("EncodeRow error: oldData, newData, columnName and tp count not match %d vs %d vs %d vs %d", len(oldRow), len(newRow), len(colName), len(tp))
	}
	values := make([]types.Datum, 4*len(oldRow))
	for i, c := range oldRow {
		values[4*i] = types.NewStringDatum(colName[i])
		values[4*i+1] = types.NewBytesDatum([]byte{tp[i]})
		values[4*i+2] = c
		values[4*i+3] = newRow[i]

	}
	if len(values) == 0 {
		// We could not set nil value into kv.
		return []byte{codec.NilFlag}, nil
	}
	return codec.EncodeValue(nil, values...)
}
