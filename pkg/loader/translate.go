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
	pb "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
	"github.com/pingcap/tidb/types"
)

// SlaveBinlogToTxn translate the Binlog format into Txn
func SlaveBinlogToTxn(binlog *pb.Binlog) (*Txn, error) {
	txn := new(Txn)
	var err error
	switch binlog.Type {
	case pb.BinlogType_DDL:
		data := binlog.DdlData
		txn.DDL = new(DDL)
		txn.DDL.Database = data.GetSchemaName()
		txn.DDL.Table = data.GetTableName()
		txn.DDL.SQL = string(data.GetDdlQuery())
	case pb.BinlogType_DML:
		for _, table := range binlog.DmlData.GetTables() {
			for _, mut := range table.GetMutations() {
				dml := new(DML)
				dml.Database = table.GetSchemaName()
				dml.Table = table.GetTableName()
				dml.Tp = getDMLType(mut)

				// setup values
				dml.Values, err = getColVals(table, mut.Row.GetColumns())
				if err != nil {
					return nil, err
				}

				// setup old values
				if dml.Tp == UpdateDMLType {
					dml.OldValues, err = getColVals(table, mut.ChangeRow.GetColumns())
					if err != nil {
						return nil, err
					}
				}
				txn.DMLs = append(txn.DMLs, dml)
			}
		}
	}
	return txn, nil
}

func getColVals(table *pb.Table, cols []*pb.Column) (map[string]interface{}, error) {
	vals := make(map[string]interface{}, len(cols))
	for i, col := range cols {
		name := table.ColumnInfo[i].Name
		arg, err := columnToArg(table.ColumnInfo[i].GetMysqlType(), col)
		if err != nil {
			return vals, err
		}
		vals[name] = arg
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
