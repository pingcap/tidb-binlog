// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package translator

import (
	"fmt"
	"io"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-binlog/pkg/util"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	tipb "github.com/pingcap/tipb/go-binlog"
)

// TiBinlogToPbBinlog translate the binlog format
func TiBinlogToPbBinlog(infoGetter TableInfoGetter, schema string, table string, tiBinlog *tipb.Binlog, pv *tipb.PrewriteValue) (pbBinlog *pb.Binlog, err error) {
	pbBinlog = new(pb.Binlog)

	pbBinlog.CommitTs = tiBinlog.CommitTs

	if tiBinlog.DdlJobId > 0 { // DDL
		sql := string(tiBinlog.GetDdlQuery())
		stmt, err := getParser().ParseOneStmt(sql, "", "")
		if err != nil {
			return nil, errors.Trace(err)
		}

		_, isCreateDatabase := stmt.(*ast.CreateDatabaseStmt)
		if isCreateDatabase {
			sql += ";"
		} else {
			sql = fmt.Sprintf("use %s; %s;", schema, sql)
		}

		pbBinlog.Tp = pb.BinlogType_DDL
		pbBinlog.DdlQuery = []byte(sql)
	} else {
		pbBinlog.DmlData = new(pb.DMLData)

		for _, mut := range pv.GetMutations() {
			var info *model.TableInfo
			var ok bool
			info, ok = infoGetter.TableByID(mut.GetTableId())
			if !ok {
				return nil, errors.Errorf("TableByID empty table id: %d", mut.GetTableId())
			}

			isTblDroppingCol := infoGetter.IsDroppingColumn(mut.GetTableId())

			schema, _, ok = infoGetter.SchemaAndTableName(mut.GetTableId())
			if !ok {
				return nil, errors.Errorf("SchemaAndTableName empty table id: %d", mut.GetTableId())
			}

			iter := newSequenceIterator(&mut)
			for {
				mutType, row, err := iter.next()
				if err != nil {
					if err == io.EOF {
						break
					}
					return nil, errors.Trace(err)
				}

				switch mutType {
				case tipb.MutationType_Insert:
					event, err := genInsert(schema, info, row)
					if err != nil {
						return nil, errors.Annotatef(err, "genInsert failed")
					}
					pbBinlog.DmlData.Events = append(pbBinlog.DmlData.Events, *event)
				case tipb.MutationType_Update:
					event, err := genUpdate(schema, info, row, isTblDroppingCol)
					if err != nil {
						return nil, errors.Annotatef(err, "genUpdate failed")
					}
					pbBinlog.DmlData.Events = append(pbBinlog.DmlData.Events, *event)

				case tipb.MutationType_DeleteRow:
					event, err := genDelete(schema, info, row)
					if err != nil {
						return nil, errors.Annotatef(err, "genDelete failed")
					}
					pbBinlog.DmlData.Events = append(pbBinlog.DmlData.Events, *event)

				default:
					return nil, errors.Errorf("unknown mutation type: %v", mutType)
				}
			}
		}
	}

	return
}

func genInsert(schema string, table *model.TableInfo, row []byte) (event *pb.Event, err error) {
	columns := table.Columns

	_, columnValues, err := insertRowToDatums(table, row)
	if err != nil {
		return nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name)
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
			return nil, errors.Trace(err)
		}
		vals = append(vals, value)
	}

	rowData, err := encodeRow(vals, cols, tps, mysqlTypes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	event = packEvent(schema, table.Name.O, pb.EventType_Insert, rowData)

	return
}

func genUpdate(schema string, table *model.TableInfo, row []byte, isTblDroppingCol bool) (event *pb.Event, err error) {
	columns := writableColumns(table)
	colsMap := util.ToColumnMap(columns)

	oldColumnValues, newColumnValues, err := DecodeOldAndNewRow(row, colsMap, time.Local, isTblDroppingCol)
	if err != nil {
		return nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name)
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
				return nil, errors.Trace(err)
			}
			newValue, err := formatData(val, col.FieldType)
			if err != nil {
				return nil, errors.Trace(err)
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
		return nil, errors.Trace(err)
	}

	event = packEvent(schema, table.Name.O, pb.EventType_Update, rowData)

	return
}

func genDelete(schema string, table *model.TableInfo, row []byte) (event *pb.Event, err error) {
	columns := table.Columns
	colsTypeMap := util.ToColumnTypeMap(columns)

	columnValues, err := tablecodec.DecodeRow(row, colsTypeMap, time.Local)
	if err != nil {
		return nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name)
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
				return nil, errors.Trace(err)
			}
			vals = append(vals, value)
			cols = append(cols, col.Name.O)
			tps = append(tps, col.Tp)
			mysqlTypes = append(mysqlTypes, types.TypeToStr(col.Tp, col.Charset))
		}
	}

	rowData, err := encodeRow(vals, cols, tps, mysqlTypes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	event = packEvent(schema, table.Name.O, pb.EventType_Delete, rowData)

	return
}

func encodeRow(row []types.Datum, colName []string, tp []byte, mysqlType []string) ([][]byte, error) {
	cols := make([][]byte, 0, len(row))
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for i, c := range row {
		val, err := codec.EncodeValue(sc, nil, []types.Datum{c}...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		col := pb.Column{
			Name:      colName[i],
			Tp:        []byte{tp[i]},
			MysqlType: mysqlType[i],
			Value:     val,
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
	cols := make([][]byte, 0, len(oldRow))
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for i, c := range oldRow {
		val, err := codec.EncodeValue(sc, nil, []types.Datum{c}...)
		if err != nil {
			return nil, errors.Trace(err)
		}

		changedVal, err := codec.EncodeValue(sc, nil, []types.Datum{newRow[i]}...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		col := pb.Column{
			Name:         colName[i],
			Tp:           []byte{tp[i]},
			MysqlType:    mysqlType[i],
			Value:        val,
			ChangedValue: changedVal,
		}

		colVal, err := col.Marshal()
		if err != nil {
			return nil, errors.Trace(err)
		}
		cols = append(cols, colVal)
	}

	return cols, nil
}

func packEvent(schemaName, tableName string, tp pb.EventType, rowData [][]byte) *pb.Event {
	event := &pb.Event{
		SchemaName: proto.String(schemaName),
		TableName:  proto.String(tableName),
		Row:        rowData,
		Tp:         tp,
	}

	return event
}
