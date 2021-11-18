package translator

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	tipb "github.com/pingcap/tipb/go-binlog"
	"io"
	"strings"
)

// TiBinlogToOracleTxn translate the format to loader.Txn
func TiBinlogToOracleTxn(infoGetter TableInfoGetter, schema string, table string, tiBinlog *tipb.Binlog, pv *tipb.PrewriteValue, shouldSkip bool) (txn *loader.Txn, err error) {
	txn = new(loader.Txn)

	if tiBinlog.DdlJobId > 0 {
		txn.DDL = &loader.DDL{
			Database:   infoGetter.ResolveDownstreamSchema(schema),
			Table:      table,
			SQL:        string(tiBinlog.GetDdlQuery()),
			ShouldSkip: shouldSkip,
		}
	} else {
		tableIDColumnsMap := make(map[int64]map[string]*model.ColumnInfo)
		for _, mut := range pv.GetMutations() {
			var info *model.TableInfo
			var ok bool
			info, ok = infoGetter.TableByID(mut.GetTableId())
			if !ok {
				return nil, errors.Errorf("TableByID empty table id: %d", mut.GetTableId())
			}

			if _, ok := tableIDColumnsMap[mut.GetTableId()]; !ok {
				tableIDColumnsMap[mut.GetTableId()] = genColumnInfoMap(info)
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
					names, args, err := genDBInsert(schema, pinfo, info, row)
					if err != nil {
						return nil, errors.Annotate(err, "gen insert fail")
					}

					dml := &loader.DML{
						Tp:               loader.InsertDMLType,
						Database:         infoGetter.ResolveDownstreamSchema(schema),
						Table:            table,
						Values:           make(map[string]interface{}),
						UpColumnsInfoMap: tableIDColumnsMap[mut.GetTableId()],
					}
					txn.DMLs = append(txn.DMLs, dml)
					for i, name := range names {
						dml.Values[strings.ToUpper(name)] = args[i]
					}
				case tipb.MutationType_Update:
					names, args, oldArgs, err := genDBUpdate(schema, pinfo, info, row, canAppendDefaultValue)
					if err != nil {
						return nil, errors.Annotate(err, "gen update fail")
					}

					dml := &loader.DML{
						Tp:               loader.UpdateDMLType,
						Database:         infoGetter.ResolveDownstreamSchema(schema),
						Table:            table,
						Values:           make(map[string]interface{}),
						OldValues:        make(map[string]interface{}),
						UpColumnsInfoMap: tableIDColumnsMap[mut.GetTableId()],
					}
					txn.DMLs = append(txn.DMLs, dml)
					for i, name := range names {
						dml.Values[strings.ToUpper(name)] = args[i]
						dml.OldValues[strings.ToUpper(name)] = oldArgs[i]
					}

				case tipb.MutationType_DeleteRow:
					names, args, err := genDBDelete(schema, info, row)
					if err != nil {
						return nil, errors.Annotate(err, "gen delete fail")
					}

					dml := &loader.DML{
						Tp:               loader.DeleteDMLType,
						Database:         infoGetter.ResolveDownstreamSchema(schema),
						Table:            table,
						Values:           make(map[string]interface{}),
						UpColumnsInfoMap: tableIDColumnsMap[mut.GetTableId()],
					}
					txn.DMLs = append(txn.DMLs, dml)
					for i, name := range names {
						dml.Values[strings.ToUpper(name)] = args[i]
					}

				default:
					return nil, errors.Errorf("unknown mutation type: %v", mutType)
				}
			}
		}
	}

	return
}

func genColumnInfoMap(table * model.TableInfo) map[string] *model.ColumnInfo {
	colsMap := make(map[string] *model.ColumnInfo)
	for _, column := range table.Columns {
		//for oracle downstream db, column name should be upper case.
		colsMap[strings.ToUpper(column.Name.O)] = column
	}
	return colsMap
}

