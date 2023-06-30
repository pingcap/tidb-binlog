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
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	tipb "github.com/pingcap/tipb/go-binlog"

	"github.com/pingcap/tidb-binlog/pkg/loader"
	"github.com/pingcap/tidb-binlog/pkg/util"
)

const implicitColID = -1

func genDBInsert(schema string, ptable, table *model.TableInfo, row []byte, destDBType loader.DBType, loc *time.Location) (names []string, args []interface{}, err error) {
	columns := writableColumns(table)

	columnValues, err := insertRowToDatums(table, row, loc)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if !ok {
			log.S().Debugf("missing col: %+v", *col)
			val = getDefaultOrZeroValue(ptable, col)
		}

		value, err := formatData(val, col.FieldType, destDBType)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		names = append(names, col.Name.O)
		args = append(args, value.GetValue())
	}

	return names, args, nil
}

func genDBUpdate(schema string, ptable, table *model.TableInfo, row []byte, canAppendDefaultValue bool, destDBType loader.DBType, loc *time.Location) (names []string, values []interface{}, oldValues []interface{}, err error) {
	columns := writableColumns(table)
	updtDecoder := newUpdateDecoder(ptable, table, canAppendDefaultValue)

	var updateColumns []*model.ColumnInfo

	oldColumnValues, newColumnValues, err := updtDecoder.decode(row, loc)
	if err != nil {
		return nil, nil, nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name)
	}

	_, oldValues, err = generateColumnAndValue(columns, oldColumnValues, destDBType)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	updateColumns, values, err = generateColumnAndValue(columns, newColumnValues, destDBType)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	names = genColumnNameList(updateColumns)

	return
}

func genDBDelete(schema string, table *model.TableInfo, row []byte, destDBType loader.DBType, loc *time.Location) (names []string, values []interface{}, err error) {
	columns := table.Columns
	colsTypeMap := util.ToColumnTypeMap(columns)

	columnValues, err := tablecodec.DecodeRowToDatumMap(row, colsTypeMap, loc)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	columns, values, err = generateColumnAndValue(columns, columnValues, destDBType)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	names = genColumnNameList(columns)

	return
}

// TiBinlogToTxn translate the format to loader.Txn
func TiBinlogToTxn(infoGetter TableInfoGetter, schema string, table string, tiBinlog *tipb.Binlog, pv *tipb.PrewriteValue, shouldSkip bool, loc *time.Location) (txn *loader.Txn, err error) {
	txn = new(loader.Txn)

	if tiBinlog.DdlJobId > 0 {
		txn.DDL = &loader.DDL{
			Database:   schema,
			Table:      table,
			SQL:        string(tiBinlog.GetDdlQuery()),
			ShouldSkip: shouldSkip,
		}
	} else {
		for _, mut := range pv.GetMutations() {
			var info *model.TableInfo
			var ok bool
			info, ok = infoGetter.TableByID(mut.GetTableId())
			if !ok {
				return nil, errors.Errorf("TableByID empty table id: %d", mut.GetTableId())
			}

			pinfo, _ := infoGetter.TableBySchemaVersion(mut.GetTableId(), pv.SchemaVersion)

			canAppendDefaultValue := infoGetter.CanAppendDefaultValue(mut.GetTableId(), pv.SchemaVersion)

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
					names, args, err := genDBInsert(schema, pinfo, info, row, loader.MysqlDB, loc)
					if err != nil {
						return nil, errors.Annotate(err, "gen insert fail")
					}

					dml := &loader.DML{
						Tp:         loader.InsertDMLType,
						Database:   schema,
						Table:      table,
						Values:     make(map[string]interface{}),
						DestDBType: loader.MysqlDB,
					}
					txn.DMLs = append(txn.DMLs, dml)
					for i, name := range names {
						dml.Values[name] = args[i]
					}
				case tipb.MutationType_Update:
					names, args, oldArgs, err := genDBUpdate(schema, pinfo, info, row, canAppendDefaultValue, loader.MysqlDB, loc)
					if err != nil {
						return nil, errors.Annotate(err, "gen update fail")
					}

					dml := &loader.DML{
						Tp:         loader.UpdateDMLType,
						Database:   schema,
						Table:      table,
						Values:     make(map[string]interface{}),
						OldValues:  make(map[string]interface{}),
						DestDBType: loader.MysqlDB,
					}
					txn.DMLs = append(txn.DMLs, dml)
					for i, name := range names {
						dml.Values[name] = args[i]
						dml.OldValues[name] = oldArgs[i]
					}

				case tipb.MutationType_DeleteRow:
					names, args, err := genDBDelete(schema, info, row, loader.MysqlDB, loc)
					if err != nil {
						return nil, errors.Annotate(err, "gen delete fail")
					}

					dml := &loader.DML{
						Tp:         loader.DeleteDMLType,
						Database:   schema,
						Table:      table,
						Values:     make(map[string]interface{}),
						DestDBType: loader.MysqlDB,
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

func generateColumnAndValue(columns []*model.ColumnInfo, columnValues map[int64]types.Datum, destDBType loader.DBType) ([]*model.ColumnInfo, []interface{}, error) {
	var newColumn []*model.ColumnInfo
	var newColumnsValues []interface{}

	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if ok {
			newColumn = append(newColumn, col)
			value, err := formatData(val, col.FieldType, destDBType)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}

			newColumnsValues = append(newColumnsValues, value.GetValue())
		}
	}

	return newColumn, newColumnsValues, nil
}

func formatData(data types.Datum, ft types.FieldType, destDBType loader.DBType) (types.Datum, error) {
	if data.GetValue() == nil {
		return data, nil
	}

	switch ft.GetType() {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp, mysql.TypeNewDecimal, mysql.TypeJSON:
		data = types.NewDatum(fmt.Sprintf("%v", data.GetValue()))
	case mysql.TypeDuration:
		//only for oracle db
		if destDBType == loader.OracleDB {
			return types.Datum{}, errors.New("unsupported column type[time]")
		}
		data = types.NewDatum(fmt.Sprintf("%v", data.GetValue()))
	case mysql.TypeEnum:
		data = types.NewDatum(data.GetMysqlEnum().Value)
	case mysql.TypeSet:
		data = types.NewDatum(data.GetMysqlSet().Value)
	case mysql.TypeBit:
		// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
		val, err := data.GetBinaryLiteral().ToInt(nil)
		if err != nil {
			return types.Datum{}, err
		}
		data = types.NewUintDatum(val)
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		//only for oracle db
		if destDBType == loader.OracleDB && isBlob(ft) {
			data = types.NewBytesDatum(data.GetBytes())
		}
	}

	return data, nil
}

func isBlob(ft types.FieldType) bool {
	stype := types.TypeToStr(ft.GetType(), ft.GetCharset())
	switch stype {
	case "blob", "tinyblob", "mediumblob", "longblob":
		return true
	}
	return false
}
