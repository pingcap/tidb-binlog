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
					arg := columnToArg(col)
					dml.Values[name] = arg
				}

				// setup old values
				if dml.Tp == UpdateDMLType {
					dml.OldValues = make(map[string]interface{})
					for i, col := range mut.ChangeRow.GetColumns() {
						name := table.ColumnInfo[i].Name
						arg := columnToArg(col)
						dml.OldValues[name] = arg
					}
				}
			}
		}
	}
	return
}

func columnToArg(c *pb.Column) (arg interface{}) {
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
		return c.GetBytesValue()
	}

	return c.GetStringValue()
}
