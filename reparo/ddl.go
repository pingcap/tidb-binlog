package repora

import (
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
)

func parseDDL(sql string) (node ast.Node, table Table, err error) {
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
			log.Warnf("node %+v is not ddl, unexpected!", n)
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

func setSchemaIfExists(table *Table, schemaName string, tableName string) {
	if schemaName != "" {
		table.Schema = schemaName
	}
	if tableName != "" {
		table.Name = tableName
	}
}
