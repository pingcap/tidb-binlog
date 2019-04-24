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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	tipb "github.com/pingcap/tipb/go-binlog"
)

const implicitColID = -1

func genMysqlInsert(schema string, table *model.TableInfo, row []byte) (names []string, args []interface{}, err error) {
	columns := writableColumns(table)

	_, columnValues, err := insertRowToDatums(table, row)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if !ok {
			val = getDefaultOrZeroValue(col)
		}

		value, err := formatData(val, col.FieldType)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		names = append(names, col.Name.O)
		args = append(args, value.GetValue())
	}

	return names, args, nil
}

func genMysqlUpdate(schema string, table *model.TableInfo, row []byte) (names []string, values []interface{}, oldValues []interface{}, err error) {
	columns := writableColumns(table)
	colsTypeMap := util.ToColumnTypeMap(columns)

	var updateColumns []*model.ColumnInfo

	oldColumnValues, newColumnValues, err := DecodeOldAndNewRow(row, colsTypeMap, time.Local)
	if err != nil {
		return nil, nil, nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name)
	}

	_, oldValues, err = generateColumnAndValue(columns, oldColumnValues)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	updateColumns, values, err = generateColumnAndValue(columns, newColumnValues)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	names = genColumnNameList(updateColumns)

	return
}

func genMysqlDelete(schema string, table *model.TableInfo, row []byte) (names []string, values []interface{}, err error) {
	columns := table.Columns
	colsTypeMap := util.ToColumnTypeMap(columns)

	columnValues, err := tablecodec.DecodeRow(row, colsTypeMap, time.Local)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	columns, values, err = generateColumnAndValue(columns, columnValues)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	names = genColumnNameList(columns)

	return
}

// TiBinlogToTxn translate the format to loader.Txn
func TiBinlogToTxn(infoGetter TableInfoGetter, schema string, table string, tiBinlog *tipb.Binlog, pv *tipb.PrewriteValue) (txn *loader.Txn, err error) {
	txn = new(loader.Txn)

	if tiBinlog.DdlJobId > 0 {
		txn.DDL = &loader.DDL{
			Database: schema,
			Table:    table,
			SQL:      string(tiBinlog.GetDdlQuery()),
		}
	} else {
		for _, mut := range pv.GetMutations() {
			var info *model.TableInfo
			var ok bool
			info, ok = infoGetter.TableByID(mut.GetTableId())
			if !ok {
				return nil, errors.Errorf("TableByID empty table id: %d", mut.GetTableId())
			}

			schema, table, ok = infoGetter.SchemaAndTableName(mut.GetTableId())
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
					names, args, err := genMysqlInsert(schema, info, row)
					if err != nil {
						return nil, errors.Annotate(err, "gen insert fail")
					}

					dml := &loader.DML{
						Tp:       loader.InsertDMLType,
						Database: schema,
						Table:    table,
						Values:   make(map[string]interface{}),
					}
					txn.DMLs = append(txn.DMLs, dml)
					for i, name := range names {
						dml.Values[name] = args[i]
					}
				case tipb.MutationType_Update:
					names, args, oldArgs, err := genMysqlUpdate(schema, info, row)
					if err != nil {
						return nil, errors.Annotate(err, "gen delete fail")
					}

					dml := &loader.DML{
						Tp:        loader.UpdateDMLType,
						Database:  schema,
						Table:     table,
						Values:    make(map[string]interface{}),
						OldValues: make(map[string]interface{}),
					}
					txn.DMLs = append(txn.DMLs, dml)
					for i, name := range names {
						dml.Values[name] = args[i]
						dml.OldValues[name] = oldArgs[i]
					}

				case tipb.MutationType_DeleteRow:
					names, args, err := genMysqlDelete(schema, info, row)
					if err != nil {
						return nil, errors.Annotate(err, "gen delete fail")
					}

					dml := &loader.DML{
						Tp:       loader.DeleteDMLType,
						Database: schema,
						Table:    table,
						Values:   make(map[string]interface{}),
					}
					txn.DMLs = append(txn.DMLs, dml)
					for i, name := range names {
						dml.Values[name] = args[i]
					}

				default:
					return nil, errors.Errorf("unknown mutation type: %v", mutType)
				}
			}
		}
	}

	return
}

// writableColumns returns all columns which can be written. This excludes
// generated and non-public columns.
func writableColumns(table *model.TableInfo) []*model.ColumnInfo {
	cols := make([]*model.ColumnInfo, 0, len(table.Columns))
	for _, col := range table.Columns {
		if col.State == model.StatePublic && !col.IsGenerated() {
			cols = append(cols, col)
		}
	}
	return cols
}

func genColumnNameList(columns []*model.ColumnInfo) (names []string) {
	for _, column := range columns {
		names = append(names, column.Name.O)
	}

	return
}

// IsPKHandleColumn check if the column if the pk handle of tidb
func IsPKHandleColumn(table *model.TableInfo, column *model.ColumnInfo) bool {
	return (mysql.HasPriKeyFlag(column.Flag) && table.PKIsHandle) || column.ID == implicitColID
}

func generateColumnAndValue(columns []*model.ColumnInfo, columnValues map[int64]types.Datum) ([]*model.ColumnInfo, []interface{}, error) {
	var newColumn []*model.ColumnInfo
	var newColumnsValues []interface{}

	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if ok {
			newColumn = append(newColumn, col)
			value, err := formatData(val, col.FieldType)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}

			newColumnsValues = append(newColumnsValues, value.GetValue())
		}
	}

	return newColumn, newColumnsValues, nil
}

func formatData(data types.Datum, ft types.FieldType) (types.Datum, error) {
	if data.GetValue() == nil {
		return data, nil
	}

	switch ft.Tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeDecimal, mysql.TypeNewDecimal, mysql.TypeJSON:
		data = types.NewDatum(fmt.Sprintf("%v", data.GetValue()))
	case mysql.TypeEnum:
		data = types.NewDatum(data.GetMysqlEnum().Value)
	case mysql.TypeSet:
		data = types.NewDatum(data.GetMysqlSet().Value)
	case mysql.TypeBit:
		data = types.NewDatum(data.GetBytes())
	}

	return data, nil
}
