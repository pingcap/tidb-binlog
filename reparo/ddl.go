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

package reparo

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-binlog/pkg/filter"
	"go.uber.org/zap"
)

func parseDDL(sql string) (node ast.Node, table filter.TableName, err error) {
	nodes, _, err := parser.New().Parse(sql, "", "")
	if err != nil {
		return nil, table, errors.Trace(err)
	}

	// we assume ddl in the following format:
	// 1. use db; ddl
	// 2. ddl  (no use statement)
	// and we assume ddl has single schema change.
	for _, n := range nodes {
		if useStmt, ok := n.(*ast.UseStmt); ok {
			table.Schema = useStmt.DBName
			continue
		}

		node = n
		//FIXME: does it needed?
		_, isDDL := n.(ast.DDLNode)
		if !isDDL {
			log.Warn("node is not ddl, unexpected!", zap.Reflect("node", n))
			continue
		}
		switch v := n.(type) {
		case *ast.CreateDatabaseStmt:
			setSchemaIfExists(&table, v.Name, "")
		case *ast.DropDatabaseStmt:
			setSchemaIfExists(&table, v.Name, "")
		case *ast.CreateTableStmt:
			setSchemaIfExists(&table, v.Table.Schema.O, v.Table.Name.O)
		case *ast.DropTableStmt:
			setSchemaIfExists(&table, v.Tables[0].Schema.O, v.Tables[0].Name.O)
		case *ast.AlterTableStmt:
			setSchemaIfExists(&table, v.Table.Schema.O, v.Table.Name.O)
		case *ast.RenameTableStmt:
			setSchemaIfExists(&table, v.OldTable.Schema.O, v.OldTable.Name.O)
		case *ast.TruncateTableStmt:
			setSchemaIfExists(&table, v.Table.Schema.O, v.Table.Name.O)
		case *ast.CreateIndexStmt:
			setSchemaIfExists(&table, v.Table.Schema.O, v.Table.Name.O)
		case *ast.DropIndexStmt:
			setSchemaIfExists(&table, v.Table.Schema.O, v.Table.Name.O)
		}
	}

	return
}

func setSchemaIfExists(table *filter.TableName, schemaName string, tableName string) {
	if schemaName != "" {
		table.Schema = schemaName
	}
	if tableName != "" {
		table.Table = tableName
	}
}
