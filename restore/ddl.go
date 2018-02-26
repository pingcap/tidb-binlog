package restore

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"
)

func parseDDL(sql string) (node ast.Node, table TableName, err error) {
	nodes, err := parser.New().Parse(sql, "", "")
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
			setSchemaIfExists(&table, v.Name)
		case *ast.DropDatabaseStmt:
			setSchemaIfExists(&table, v.Name)
		case *ast.CreateTableStmt:
			setSchemaIfExists(&table, v.Table.Schema.O)
			table.Name = v.Table.Name.O
		case *ast.DropTableStmt:
			setSchemaIfExists(&table, v.Tables[0].Schema.O)
			table.Name = v.Tables[0].Name.O
		case *ast.AlterTableStmt:
			setSchemaIfExists(&table, v.Table.Schema.O)
			table.Name = v.Table.Name.O
		case *ast.RenameTableStmt:
			setSchemaIfExists(&table, v.OldTable.Schema.O)
			table.Name = v.OldTable.Name.O
		case *ast.TruncateTableStmt:
			setSchemaIfExists(&table, v.Table.Schema.O)
			table.Name = v.Table.Name.O
		case *ast.CreateIndexStmt:
			setSchemaIfExists(&table, v.Table.Schema.O)
			table.Name = v.Table.Name.O
		case *ast.DropIndexStmt:
			setSchemaIfExists(&table, v.Table.Schema.O)
			table.Name = v.Table.Name.O
		}
	}

	return
}

func setSchemaIfExists(table *TableName, schema string) {
	if schema != "" {
		table.Schema = schema
	}
}
