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
)

// SlaveBinlogToTxn translate the Binlog format into Txn
func SlaveBinlogToTxn(binlog *pb.Binlog) (txn *Txn) {
	txn = new(Txn)
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
				txn.DMLs = append(txn.DMLs, dml)
				dml.Database = table.GetSchemaName()
				dml.Table = table.GetTableName()
				switch mut.GetType() {
				case pb.MutationType_Insert:
					dml.Tp = InsertDMLType
				case pb.MutationType_Update:
					dml.Tp = UpdateDMLType
				case pb.MutationType_Delete:
					dml.Tp = DeleteDMLType
				}

				// setup values
				dml.Values = make(map[string]interface{})
				for i, col := range mut.Row.GetColumns() {
					name := table.ColumnInfo[i].Name
					arg := columnToArg(table.ColumnInfo[i].GetMysqlType(), col)
					dml.Values[name] = arg
				}

				// setup old values
				if dml.Tp == UpdateDMLType {
					dml.OldValues = make(map[string]interface{})
					for i, col := range mut.ChangeRow.GetColumns() {
						name := table.ColumnInfo[i].Name
						arg := columnToArg(table.ColumnInfo[i].GetMysqlType(), col)
						dml.OldValues[name] = arg
					}
				}
			}
		}
	}
	return
}

func columnToArg(mysqlType string, c *pb.Column) (arg interface{}) {
	if c.GetIsNull() {
		return nil
	}

	if c.Int64Value != nil {
		return c.GetInt64Value()
	}

	if c.Uint64Value != nil {
		return c.GetUint64Value()
	}

	if c.DoubleValue != nil {
		return c.GetDoubleValue()
	}

	if c.BytesValue != nil {
		// https://github.com/go-sql-driver/mysql/issues/819
		// for downstream = mysql
		// it work for tidb to use binary
		if mysqlType == "json" {
			var str string = string(c.GetBytesValue())
			return str
		}
		return c.GetBytesValue()
	}

	return c.GetStringValue()
}
