package translator

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	tipb "github.com/pingcap/tipb/go-binlog"
	"io"
	"sort"
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
		tableIdColumnsMap := make(map[int64]map[string]*model.ColumnInfo)
		tableIdIndexsMap := make(map[int64][]*model.IndexInfo)
		for _, mut := range pv.GetMutations() {
			var info *model.TableInfo
			var ok bool
			info, ok = infoGetter.TableByID(mut.GetTableId())
			if !ok {
				return nil, errors.Errorf("TableByID empty table id: %d", mut.GetTableId())
			}

			if _, ok := tableIdColumnsMap[mut.GetTableId()]; !ok {
				tableIdColumnsMap[mut.GetTableId()] = genColumnInfoMap(info)
				tableIdIndexsMap[mut.GetTableId()] = FindAllIndex(info)
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
					names, args, err := genMysqlInsert(schema, pinfo, info, row)
					if err != nil {
						return nil, errors.Annotate(err, "gen insert fail")
					}

					dml := &loader.DML{
						Tp:       loader.InsertDMLType,
						Database: infoGetter.ResolveDownstreamSchema(schema),
						Table:    table,
						Values:   make(map[string]interface{}),
						UpColumnsInfoMap: tableIdColumnsMap[mut.GetTableId()],
						UpIndexs: tableIdIndexsMap[mut.GetTableId()],
						UpInfo: info,
					}
					txn.DMLs = append(txn.DMLs, dml)
					for i, name := range names {
						dml.Values[name] = args[i]
					}
				case tipb.MutationType_Update:
					names, args, oldArgs, err := genMysqlUpdate(schema, pinfo, info, row, canAppendDefaultValue)
					if err != nil {
						return nil, errors.Annotate(err, "gen update fail")
					}

					dml := &loader.DML{
						Tp:        loader.UpdateDMLType,
						Database:  infoGetter.ResolveDownstreamSchema(schema),
						Table:     table,
						Values:    make(map[string]interface{}),
						OldValues: make(map[string]interface{}),
						UpColumnsInfoMap: tableIdColumnsMap[mut.GetTableId()],
						UpIndexs: tableIdIndexsMap[mut.GetTableId()],
						UpInfo: info,
					}
					txn.DMLs = append(txn.DMLs, dml)
					for i, name := range names {
						dml.Values[name] = args[i]
						dml.OldValues[name] = oldArgs[i]
					}

				case tipb.MutationType_DeleteRow:
					names, args, err := genMysqlDelete(schema, info, row)
					if err != nil {
						return nil, errors.Annotate(err, "gen delete fail")
					}

					dml := &loader.DML{
						Tp:       loader.DeleteDMLType,
						Database: infoGetter.ResolveDownstreamSchema(schema),
						Table:    table,
						Values:   make(map[string]interface{}),
						UpColumnsInfoMap: tableIdColumnsMap[mut.GetTableId()],
						UpIndexs: tableIdIndexsMap[mut.GetTableId()],
						UpInfo: info,
					}
					txn.DMLs = append(txn.DMLs, dml)
					for i, name := range names {
						dml.Values[name] = args[i]
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
		colsMap[column.Name.O] = column
	}
	return colsMap
}

// FindAllIndex returns all index, order is pk, uk, and normal index.
func FindAllIndex(tableInfo *model.TableInfo) []*model.IndexInfo {
	indices := make([]*model.IndexInfo, len(tableInfo.Indices))
	copy(indices, tableInfo.Indices)
	sort.SliceStable(indices, func(i, j int) bool {
		a := indices[i]
		b := indices[j]
		switch {
		case b.Primary:
			return false
		case a.Primary:
			return true
		case b.Unique:
			return false
		case a.Unique:
			return true
		default:
			return false
		}
	})
	return indices
}

