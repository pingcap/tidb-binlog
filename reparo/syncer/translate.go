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

package syncer

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver" // for parser driver
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-binlog/pkg/loader"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

func trimUse(sql string) string {
	split := strings.Split(sql, ";")
	if len(split) > 0 && strings.HasPrefix(split[0], "use ") {
		return strings.Join(split[1:], ";")
	}

	return sql
}

func pbBinlogToTxn(binlog *pb.Binlog) (txn *loader.Txn, err error) {
	txn = new(loader.Txn)
	switch binlog.Tp {
	case pb.BinlogType_DDL:
		txn.DDL = new(loader.DDL)
		// If it is not create database statement, pb.Binlog.DdlQuery will be "use <db>; create..."
		// trim the "use..." to make it as expected by `loader.Txn`
		// we should better just pass the database context into pb.Binlog
		txn.DDL.SQL = trimUse(string(binlog.DdlQuery))
		txn.DDL.Database, txn.DDL.Table, err = parserSchemaTableFromDDL(string(binlog.DdlQuery))
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(txn.DDL.Database) == 0 {
			return nil, errors.Errorf("can't parse database name from DDL %s", binlog.DdlQuery)
		}
	case pb.BinlogType_DML:
		data := binlog.DmlData
		for _, event := range data.GetEvents() {
			dml := new(loader.DML)
			dml.Database = event.GetSchemaName()
			dml.Table = event.GetTableName()
			txn.DMLs = append(txn.DMLs, dml)

			switch event.GetTp() {
			case pb.EventType_Insert:
				dml.Tp = loader.InsertDMLType

				cols, args, err := genColsAndArgs(event.Row)
				if err != nil {
					return nil, errors.Trace(err)
				}

				dml.Values = make(map[string]interface{})
				for i := 0; i < len(cols); i++ {
					dml.Values[cols[i]] = args[i]
				}
			case pb.EventType_Update:
				dml.Tp = loader.UpdateDMLType
				dml.Values = make(map[string]interface{})
				dml.OldValues = make(map[string]interface{})

				for _, c := range event.GetRow() {
					col := &pb.Column{}
					err := col.Unmarshal(c)
					if err != nil {
						return nil, errors.Trace(err)
					}

					_, oldDatum, err := codec.DecodeOne(col.Value)
					if err != nil {
						return nil, errors.Trace(err)
					}
					_, newDatum, err := codec.DecodeOne(col.ChangedValue)
					if err != nil {
						return nil, errors.Trace(err)
					}

					tp := col.Tp[0]
					newDatum = formatValue(newDatum, tp)
					newValue := newDatum.GetValue()
					oldDatum = formatValue(oldDatum, tp)
					oldValue := oldDatum.GetValue()

					log.Debug("translate update event",
						zap.String("col name", col.Name),
						zap.String("col mysql type", col.MysqlType),
						zap.Uint8("tp", tp),
						zap.Reflect("old value", oldValue),
						zap.Reflect("new value", newValue))

					dml.Values[col.Name] = newValue
					dml.OldValues[col.Name] = oldValue
				}
			case pb.EventType_Delete:
				dml.Tp = loader.DeleteDMLType

				cols, args, err := genColsAndArgs(event.Row)
				if err != nil {
					return nil, errors.Trace(err)
				}

				dml.Values = make(map[string]interface{})
				for i := 0; i < len(cols); i++ {
					dml.Values[cols[i]] = args[i]
				}
			default:
				return nil, errors.Errorf("unknown type: %v", event.GetTp())
			}
		}
	default:
		return nil, errors.Errorf("unknown type: %v", binlog.Tp)
	}

	return
}

func genColsAndArgs(row [][]byte) (cols []string, args []interface{}, err error) {
	cols = make([]string, 0, len(row))
	args = make([]interface{}, 0, len(row))
	for _, c := range row {
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		cols = append(cols, col.Name)

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		tp := col.Tp[0]
		val = formatValue(val, tp)
		log.Debug("format value",
			zap.String("col name", col.Name),
			zap.String("mysql type", col.MysqlType),
			zap.Reflect("value", val.GetValue()))
		args = append(args, val.GetValue())
	}

	return
}

// parserSchemaTableFromDDL parses ddl query to get schema and table
// ddl like `use test; create table`
func parserSchemaTableFromDDL(ddlQuery string) (schema, table string, err error) {
	stmts, _, err := parser.New().Parse(ddlQuery, "", "")
	if err != nil {
		return "", "", err
	}

	haveUseStmt := false

	for _, stmt := range stmts {
		switch node := stmt.(type) {
		case *ast.UseStmt:
			haveUseStmt = true
			schema = node.DBName
		case *ast.CreateDatabaseStmt:
			schema = node.Name.O
		case *ast.DropDatabaseStmt:
			schema = node.Name.O
		case *ast.TruncateTableStmt:
			if len(node.Table.Schema.O) != 0 {
				schema = node.Table.Schema.O
			}
			table = node.Table.Name.O
		case *ast.CreateIndexStmt:
			if len(node.Table.Schema.O) != 0 {
				schema = node.Table.Schema.O
			}
			table = node.Table.Name.O
		case *ast.CreateTableStmt:
			if len(node.Table.Schema.O) != 0 {
				schema = node.Table.Schema.O
			}
			table = node.Table.Name.O
		case *ast.DropIndexStmt:
			if len(node.Table.Schema.O) != 0 {
				schema = node.Table.Schema.O
			}
			table = node.Table.Name.O
		case *ast.AlterTableStmt:
			if len(node.Table.Schema.O) != 0 {
				schema = node.Table.Schema.O
			}
			table = node.Table.Name.O
		case *ast.DropTableStmt:
			// FIXME: may drop more than one table in a ddl
			if len(node.Tables[0].Schema.O) != 0 {
				schema = node.Tables[0].Schema.O
			}
			table = node.Tables[0].Name.O
		case *ast.RenameTableStmt:
			if len(node.TableToTables[0].NewTable.Schema.O) != 0 {
				schema = node.TableToTables[0].NewTable.Schema.O
			}
			table = node.TableToTables[0].NewTable.Name.O
		case *ast.CreateViewStmt:
			if len(node.ViewName.Schema.O) != 0 {
				schema = node.ViewName.Schema.O
			}
			table = node.ViewName.Name.O
		default:
			return "", "", errors.Errorf("unknown ddl type, ddl: %s", ddlQuery)
		}
	}

	if haveUseStmt {
		if len(stmts) != 2 {
			return "", "", errors.Errorf("invalid ddl %s", ddlQuery)
		}
	} else {
		if len(stmts) != 1 {
			return "", "", errors.Errorf("invalid ddl %s", ddlQuery)
		}
	}

	return
}
