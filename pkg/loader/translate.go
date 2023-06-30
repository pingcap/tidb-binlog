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

package loader

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	router "github.com/pingcap/tidb-tools/pkg/table-router"

	"github.com/pingcap/tidb/parser/model"
	ptypes "github.com/pingcap/tidb/parser/types"
	pb "github.com/pingcap/tidb/tidb-binlog/proto/go-binlog"
	"github.com/pingcap/tidb/types"
)

// SecondaryBinlogToTxn translate the Binlog format into Txn
func SecondaryBinlogToTxn(binlog *pb.Binlog, tableRouter *router.Table, upperColName bool) (*Txn, error) {
	txn := new(Txn)
	var err error
	downStreamSchema := ""
	downStreamTable := ""
	switch binlog.Type {
	case pb.BinlogType_DDL:
		data := binlog.DdlData
		downStreamSchema = data.GetSchemaName()
		downStreamTable = data.GetTableName()
		if tableRouter != nil {
			downStreamSchema, downStreamTable, err = tableRouter.Route(data.GetSchemaName(), data.GetTableName())
			if err != nil {
				return nil, errors.Annotate(err, fmt.Sprintf("gen route schema and table failed. schema=%s, table=%s", data.GetSchemaName(), data.GetTableName()))
			}
		}
		txn.DDL = new(DDL)
		txn.DDL.Database = downStreamSchema
		txn.DDL.Table = downStreamTable
		txn.DDL.SQL = string(data.GetDdlQuery())
	case pb.BinlogType_DML:
		for _, table := range binlog.DmlData.GetTables() {
			downStreamSchema = table.GetSchemaName()
			downStreamTable = table.GetTableName()
			if tableRouter != nil {
				downStreamSchema, downStreamTable, err = tableRouter.Route(table.GetSchemaName(), table.GetTableName())
				if err != nil {
					return nil, errors.Annotate(err, fmt.Sprintf("gen route schema and table failed. schema=%s, table=%s", table.GetSchemaName(), table.GetTableName()))
				}
			}
			for _, mut := range table.GetMutations() {
				dml := new(DML)
				dml.Database = downStreamSchema
				dml.Table = downStreamTable
				dml.Tp = getDMLType(mut)

				// setup values
				dml.Values, err = getColVals(table, mut.Row.GetColumns(), upperColName)
				if err != nil {
					return nil, err
				}

				// setup old values
				if dml.Tp == UpdateDMLType {
					dml.OldValues, err = getColVals(table, mut.ChangeRow.GetColumns(), upperColName)
					if err != nil {
						return nil, err
					}
				}
				//only for oracle
				if upperColName {
					dml.UpColumnsInfoMap = getColumnsInfoMap(table.ColumnInfo)
				}
				txn.DMLs = append(txn.DMLs, dml)
			}
		}
	}
	return txn, nil
}

func getColumnsInfoMap(columnInfos []*pb.ColumnInfo) map[string]*model.ColumnInfo {
	colMap := make(map[string]*model.ColumnInfo)
	for _, col := range columnInfos {
		colMap[strings.ToUpper(col.Name)] = &model.ColumnInfo{
			Name: model.CIStr{O: col.Name},
			FieldType: types.NewFieldTypeBuilder().
				SetType(ptypes.StrToType(col.MysqlType)).
				SetFlen(int(col.Flen)).
				SetDecimal(int(col.Decimal)).
				Build(),
		}
	}
	return colMap
}

func getColVals(table *pb.Table, cols []*pb.Column, upperColName bool) (map[string]interface{}, error) {
	vals := make(map[string]interface{}, len(cols))
	for i, col := range cols {
		name := table.ColumnInfo[i].Name
		arg, err := columnToArg(table.ColumnInfo[i].GetMysqlType(), col)
		if err != nil {
			return vals, err
		}
		if upperColName {
			vals[strings.ToUpper(name)] = arg
		} else {
			vals[name] = arg
		}
	}
	return vals, nil
}

func columnToArg(mysqlType string, c *pb.Column) (arg interface{}, err error) {
	if c.GetIsNull() {
		return nil, nil
	}

	if c.Int64Value != nil {
		return c.GetInt64Value(), nil
	}

	if c.Uint64Value != nil {
		return c.GetUint64Value(), nil
	}

	if c.DoubleValue != nil {
		return c.GetDoubleValue(), nil
	}

	if c.BytesValue != nil {
		// https://github.com/go-sql-driver/mysql/issues/819
		// for downstream = mysql
		// it work for tidb to use binary
		if mysqlType == "json" {
			var str string = string(c.GetBytesValue())
			return str, nil
		}
		// https://github.com/pingcap/tidb/issues/10988
		// Binary literal is not passed correctly for TiDB in some cases, so encode BIT types as integers instead of byte strings. Since the longest value is BIT(64), it is safe to always convert as uint64
		if mysqlType == "bit" {
			val, err := types.BinaryLiteral(c.GetBytesValue()).ToInt(nil)
			return val, err
		}
		return c.GetBytesValue(), nil
	}

	return c.GetStringValue(), nil
}

func getDMLType(mut *pb.TableMutation) DMLType {
	switch mut.GetType() {
	case pb.MutationType_Insert:
		return InsertDMLType
	case pb.MutationType_Update:
		return UpdateDMLType
	case pb.MutationType_Delete:
		return DeleteDMLType
	default:
		return UnknownDMLType
	}
}
