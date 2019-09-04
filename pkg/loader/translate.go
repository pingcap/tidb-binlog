package loader

import (
	pb "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
	"github.com/pingcap/tidb/types"
)

// SlaveBinlogToTxn translate the Binlog format into Txn
func SlaveBinlogToTxn(binlog *pb.Binlog) (*Txn, error) {
	txn := new(Txn)
	var err error
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
					dml.Values[name], err = columnToArg(table.ColumnInfo[i].GetMysqlType(), col)
					if err != nil {
						return nil, err
					}
				}

				// setup old values
				if dml.Tp == UpdateDMLType {
					dml.OldValues = make(map[string]interface{})
					for i, col := range mut.ChangeRow.GetColumns() {
						name := table.ColumnInfo[i].Name
						dml.OldValues[name], err = columnToArg(table.ColumnInfo[i].GetMysqlType(), col)
						if err != nil {
							return nil, err
						}
					}
				}
			}
		}
	}
	return txn, nil
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
